import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from dds_loader.dds_message_processor_job import DdsMessageProcessor
from dds_loader.dds_message_processor_job import DdsRepository

app = Flask(__name__)

config = AppConfig()


@app.get('/health')
def hello_world():
    return 'healthy'


if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG)

    kafka_consumer = config.kafka_consumer()
    kafka_producer = config.kafka_producer()
    pg_db = DdsRepository(config.pg_warehouse_db())

    proc = DdsMessageProcessor(
        kafka_consumer,
        kafka_producer,
        pg_db,
        app.logger
    )

    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=25)
    scheduler.start()

    app.run(debug=True, host='0.0.0.0', use_reloader=False)
