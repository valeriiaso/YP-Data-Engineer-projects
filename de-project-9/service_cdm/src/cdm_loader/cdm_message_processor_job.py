from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 logger: Logger,
                 ) -> None:

        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg_in = self._consumer.consume()

            if not msg_in:
                self._logger.info(f"{datetime.utcnow()}: No messages. Quitting")
                break


            user_id = msg_in['payload']['user_id']
            product_id = msg_in['payload']['product_id']
            product_name = msg_in['payload']['product_name']
            category_id = msg_in['payload']['category_id']
            category_name = msg_in['payload']['category_name']


            self._cdm_repository.user_category_counters_insert(user_id, category_id, category_name)
            self._cdm_repository.user_product_counters_insert(user_id, product_id, product_name)

            self._logger.info(f"{datetime.utcnow()}: data is added to CDM")


        self._logger.info(f"{datetime.utcnow()}: FINISH")
