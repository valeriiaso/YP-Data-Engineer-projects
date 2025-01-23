from logging import Logger 

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class FctObject(BaseModel):
    id: int
    courier_id: int
    order_id: int
    rate: int
    order_sum: float
    tips_sum: float


class FctDestRepository:

    def list_facts(self, conn: Connection, fact_threshold: int, limit: int):
        with conn.cursor(row_factory=class_row(FctObject)) as cur:
            cur.execute(
                """
                    SELECT	do2.id,
                            do2.courier_id,
                            do2.id AS order_id,
                            tbl.rate,
                            tbl.order_sum,
                            tbl.tips_sum
                    FROM
                        (SELECT	object_value::JSON->>'courier_id' AS courier_id,
                                object_value::JSON->>'order_id' AS order_id,	
                                CAST(object_value::JSON->>'rate' AS int) AS rate,
                                CAST(object_value::JSON->>'sum' AS float) AS order_sum,
                                CAST(object_value::JSON->>'tip_sum' AS float) AS tips_sum
                        FROM stg.deliveries d 
                        ) tbl
                    JOIN dds.dm_orders do2 
                    ON tbl.order_id = do2.order_key
                    WHERE do2.id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """,
                {
                    "threshold": fact_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


    def insert_facts(self, conn: Connection, fact: FctObject):
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_couriers_sale(courier_id, order_id, rate, order_sum, tips_sum)
                    VALUES(%(courier_id)s, %(order_id)s, %(rate)s, %(order_sum)s, %(tips_sum)s)
                """,
                {
                    "courier_id": fact.courier_id,
                    "order_id": fact.order_id,
                    "rate": fact.rate,
                    "order_sum": fact.order_sum,
                    "tips_sum": fact.tips_sum
                }
            )


class FactLoader:
    WF_KEY = 'fact_dds_workflow'
    LAST_LOADED_ID_KEY = 'last_loaded_id'
    BATCH_LIMIT = 100

    def __init__(self, pg_dest: PgConnect, log: Logger):
        self.pg_dest = pg_dest
        self.stg = FctDestRepository()
        self.setting_repository = DdsEtlSettingsRepository() 
        self.log = log


    def load_fact(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.setting_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_facts(conn, last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} facts to load")

            if not load_queue:
                self.log.info("Quitting")
                return
            
            for fact in load_queue:
                self.stg.insert_facts(conn, fact)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([c.id for c in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.setting_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Loading is finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")