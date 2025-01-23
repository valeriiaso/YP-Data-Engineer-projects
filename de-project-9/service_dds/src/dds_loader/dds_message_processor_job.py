from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository import DdsRepository

import uuid
import json


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 logger: Logger) -> None:

        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger

        self._batch_size = 30

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg_in = self._consumer.consume()
            
            if not msg_in:
                self._logger.info(f"{datetime.utcnow()}: No messages. Quitting")
                break
            self._logger.info(f"{datetime.utcnow()}: {msg_in}")

            if 'object_type' not in msg_in:
                self._logger.info(f"{datetime.utcnow()}: no 'object_type' in {msg_in}")
                continue

            if msg_in['object_type'] != 'order':
                self._logger.info(f"{datetime.utcnow()}: message is not of type 'order'")
                continue

            load_date = datetime.utcnow()
            source = 'orders_kafka'

            # Обработка пользователей
            user_id = msg_in['payload']['user']['id']
            h_user_pk = uuid.uuid3(uuid.NAMESPACE_X500, user_id)
            username = msg_in['payload']['user']['name']
            userlogin = username
            hk_user_names_hashdiff = uuid.uuid3(uuid.NAMESPACE_X500, user_id + username + userlogin)
            if 'login' in msg_in['payload']['user']:
                userlogin = msg_in['payload']['user']['login']
            
            self._dds_repository.h_user_insert(h_user_pk, user_id, load_date, source)
            self._dds_repository.s_user_names_insert(h_user_pk, hk_user_names_hashdiff, username, userlogin, load_date, source)

            # Обработка ресторанов
            restaurant_id = msg_in['payload']['restaurant']['id']
            h_restaurant_pk = uuid.uuid3(uuid.NAMESPACE_X500, restaurant_id)
            restaurant_name = msg_in['payload']['restaurant']['name']
            hk_restaurant_names_hashdiff = uuid.uuid3(uuid.NAMESPACE_X500, restaurant_id + restaurant_name)
            
            self._dds_repository.h_restaurant_insert(h_restaurant_pk, restaurant_id, load_date, source)
            self._dds_repository.s_restaurant_names_insert(h_restaurant_pk, hk_restaurant_names_hashdiff, restaurant_name, load_date, source)

            # Обработка заказов
            order_id = msg_in['payload']['id']
            h_order_pk = uuid.uuid3(uuid.NAMESPACE_X500, str(order_id))
            order_dt = msg_in['payload']['date']
            order_cost = msg_in['payload']['cost']
            order_payment = msg_in['payload']['payment']
            order_status = msg_in['payload']['status']
            hk_order_cost_hashdiff = uuid.uuid3(uuid.NAMESPACE_X500, str(order_id) + str(order_cost) + str(order_payment))
            hk_order_status_hashdiff = uuid.uuid3(uuid.NAMESPACE_X500, str(order_id) + order_status)
            
            self._dds_repository.h_order_insert(h_order_pk, order_id, order_dt, load_date, source)
            self._dds_repository.s_order_cost_insert(h_order_pk, hk_order_cost_hashdiff, order_cost, order_payment, load_date, source)
            self._dds_repository.s_order_status_insert(h_order_pk, hk_order_status_hashdiff, order_status, load_date, source)


            hk_order_user_pk = uuid.uuid3(uuid.NAMESPACE_X500, str(h_order_pk) + str(h_user_pk))
            self._dds_repository.l_order_user_insert(hk_order_user_pk, h_order_pk, h_user_pk, load_date, source)


            for product in msg_in['payload']['products']:
                category_name = product['category']
                h_category_pk = uuid.uuid3(uuid.NAMESPACE_X500, category_name)
                self._dds_repository.h_category_insert(h_category_pk, category_name, load_date, source)

                product_id = product['id']
                product_name = product['name']
                h_product_pk = uuid.uuid3(uuid.NAMESPACE_X500, product_id)
                hk_product_names_hashdiff = uuid.uuid3(uuid.NAMESPACE_X500, product_id + product_name)
                self._dds_repository.h_product_insert(h_product_pk, product_id, load_date, source)
                self._dds_repository.s_product_names_insert(h_product_pk, hk_product_names_hashdiff, product_name, load_date, source)

                hk_product_category_pk = uuid.uuid3(uuid.NAMESPACE_X500, str(h_product_pk) + str(h_category_pk))
                self._dds_repository.l_product_category_insert(hk_product_category_pk, h_product_pk, h_category_pk, load_date, source)

                hk_product_restaurant_pk = uuid.uuid3(uuid.NAMESPACE_X500, str(h_product_pk) + str(h_restaurant_pk))
                self._dds_repository.l_product_restaurant_insert(hk_product_restaurant_pk, h_product_pk, h_restaurant_pk, load_date, source)

                hk_order_product_pk = uuid.uuid3(uuid.NAMESPACE_X500, str(h_order_pk) + str(h_product_pk))
                self._dds_repository.l_order_product_insert(hk_order_product_pk, h_order_pk, h_product_pk, load_date, source)


                if order_status == 'CLOSED':
                    msg_out = {
                            "object_id": str(uuid.uuid4()),
                            "object_type": 'cdm_message',
                            "payload": {
                                "order_id": str(h_order_pk),
                                "user_id": str(h_user_pk),
                                "product_id": str(h_product_pk),
                                "product_name": product_name,
                                "category_id": str(h_category_pk),
                                "category_name": category_name
                                }
                            }

                    self._producer.produce(msg_out)
                    self._logger.info(f"{datetime.utcnow()}: message sent to CDM")


        self._logger.info(f"{datetime.utcnow()}: FINISH")
