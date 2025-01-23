from logging import Logger 

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class RateObj(BaseModel):
    id: int
    courier_id: int
    date_id: int
    rate: int


class RateDestRepository:

    def list_rates(self, conn: Connection, rates_threshold: int, limit: int):
        with conn.cursor(row_factory=class_row(RateObj)) as cur:
            cur.execute(
                """
                    SELECT	tbl_r.id,
                            dc.id AS courier_id,
                            dt.id AS date_id,
                            tbl_r.rate
                    FROM
                    (SELECT	id,
                            object_value::JSON->>'courier_id' AS courier_id,
                            object_value::JSON->>'rate' AS rate,
                            CAST(object_value::JSON->>'order_ts' AS timestamp(0)) AS order_ts
                    FROM stg.deliveries) AS tbl_r
                    JOIN dds.dm_couriers dc 
                    ON tbl_r.courier_id = dc.courier_id 
                    JOIN dds.dm_timestamps dt 
                    ON tbl_r.order_ts = dt.ts
                    WHERE tbl_r.id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """,
                {
                    "threshold": rates_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs
    

    def insert_rates(self, conn: Connection, rate: RateObj):
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_rates(courier_id, date_id, rate)
                    VALUES(%(courier_id)s, %(date_id)s, %(rate)s)
                """,
                {
                    "courier_id": rate.courier_id,
                    "date_id": rate.date_id,
                    "rate": rate.rate
                }
            )


class RateLoader:
    WF_KEY = 'rates_stg_to_dds_workflow'
    LAST_LOADED_ID_KEY = 'last_loaded_id'
    BATCH_LIMIT = 200


    def __init__(self, pg_dest: PgConnect, log: Logger):
        self.pg_dest = pg_dest
        self.stg = RateDestRepository()
        self.setting_repository = DdsEtlSettingsRepository()
        self.log = log


    def load_rate(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.setting_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_rates(conn, last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} rates to load")

            if not load_queue:
                self.log.info("Quitting")
                return
            
            for rate in load_queue:
                self.stg.insert_rates(conn, rate)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([c.id for c in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.setting_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Loading is finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
