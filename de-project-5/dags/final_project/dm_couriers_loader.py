from logging import Logger 

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class CourierObj(BaseModel):
    id: int
    courier_id: str
    courier_name: str


class CourierDestRepository:

    def list_courier(self, conn: Connection, courier_threshold: int, limit: int):
        with conn.cursor(row_factory=class_row(CourierObj)) as cur:
            cur.execute(
                """
                    SELECT	id,
                            object_value::JSON->>'_id' AS courier_id,
		                    object_value::JSON->>'name' AS courier_name
                    FROM stg.couriers c
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """,
                {
                    "threshold": courier_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs
    

    def insert_courier(self, conn:Connection, courier: CourierObj):
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_couriers(courier_id, courier_name)
                    VALUES(%(courier_id)s, %(courier_name)s)
                """,
                {
                    "courier_id": courier.courier_id,
                    "courier_name": courier.courier_name
                }
            )


class CourierLoader:
    WF_KEY = 'couriers_stg_to_dds_workflow'
    LAST_LOADED_ID_KEY = 'last_loaded_id'
    BATCH_LIMIT = 100

    def __init__(self, pg_dest: PgConnect, log: Logger):
        self.pg_dest = pg_dest
        self.stg = CourierDestRepository()
        self.setting_repository = DdsEtlSettingsRepository() 
        self.log = log


    def load_courier(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.setting_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_courier(conn, last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} couriers to load")

            if not load_queue:
                self.log.info("Quitting")
                return
            
            for courier in load_queue:
                self.stg.insert_courier(conn, courier)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([c.id for c in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.setting_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Loading is finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")