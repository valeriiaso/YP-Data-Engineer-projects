import json

from datetime import datetime
from logging import Logger
from lib import PgConnect
from examples.stg import EtlSetting, StgEtlSettingsRepository
from final_project.pg_saver_couriers import PgSaverC
from lib.dict_util import json2str


class CouriersLoader:
    WF_KEY = "api_couriers_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, pg_dest: PgConnect, pg_saver: PgSaverC, logger: Logger):
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.setting_repository = StgEtlSettingsRepository()
        self.log = logger

    
    def run_copy(self, data):
        with self.pg_dest.connection() as conn:
            wf_settings = self.setting_repository.get_setting(conn, self.WF_KEY)
            if not wf_settings:
                wf_settings = EtlSetting(
                    id = 0,
                    workflow_key = self.WF_KEY,
                    workflow_settings = {self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()}
                )

            load_queue = data
            self.log.info(f"Found {len(load_queue)} documents to sync from GET/couriers")
            if not load_queue:
                self.log.info("Quitting")
                return 0
            
            update_ts = datetime.now()
            for d in load_queue:
                self.pg_saver.save_object(conn, str(d["_id"]), d, update_ts)

            wf_settings.workflow_settings[self.LAST_LOADED_TS_KEY] = update_ts
            wf_setting_json = json2str(wf_settings.workflow_settings)
            self.setting_repository.save_setting(conn, wf_settings.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

