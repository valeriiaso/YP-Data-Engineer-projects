from logging import Logger 

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DeliveriesObj(BaseModel):
    id: int
    order_id: int
    delivery_id: str
    address: str


class DeliveriesDestRepository:
    
    def list_deliveries(self, conn: Connection, deliveries_threshold: int, limit: int):
        with conn.cursor(row_factory=class_row(DeliveriesObj)) as cur:
            cur.execute(
                """
                    SELECT	tbl_d.id,
                            do2.id AS order_id,
                            tbl_d.delivery_id,
                            tbl_d.address
                    FROM
                    (SELECT	id,
                            object_value::JSON->>'order_id' AS order_id,
                            object_value::JSON->>'delivery_id' as delivery_id,	
                            object_value::JSON->>'address' as address
                    FROM stg.deliveries) AS tbl_d 
                    JOIN dds.dm_orders do2 
                    ON tbl_d.order_id = do2.order_key
                    WHERE tbl_d.id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """,
                {
                    "threshold": deliveries_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs
    

    def insert_deliveries(self, conn: Connection, delivery: DeliveriesObj):
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries(order_id, delivery_id, address)
                    VALUES(%(order_id)s, %(delivery_id)s, %(address)s)
                """,
                {
                    "order_id": delivery.order_id,
                    "delivery_id": delivery.delivery_id,
                    "address": delivery.address
                }
            )


class DeliveriesLoaderDm:
    WF_KEY = 'deliveries_stg_to_dds_workflow'
    LAST_LOADED_ID_KEY = 'last_loaded_id'
    BATCH_LIMIT = 200

    def __init__(self, pg_dest: PgConnect, log: Logger):
        self.pg_dest = pg_dest
        self.stg = DeliveriesDestRepository()
        self.setting_repository = DdsEtlSettingsRepository()
        self.log = log
    

    def load_delivery(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.setting_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_deliveries(conn, last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load")

            if not load_queue:
                self.log.info("Quitting")
                return
            
            for delivery in load_queue:
                self.stg.insert_deliveries(conn, delivery)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([c.id for c in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.setting_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Loading is finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
