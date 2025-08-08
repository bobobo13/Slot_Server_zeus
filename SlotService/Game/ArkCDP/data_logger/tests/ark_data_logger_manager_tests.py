import time
import unittest

from mock import Mock

from ..ark_data_logger_manager import ArkDataLoggerManager
from ..modules.kafka_logger import KafkaLogger
from ..modules.mongo_logger import MongoLogger


class ArkDataLoggerManagerTests(unittest.TestCase):
    def setUp(self):
        self.config = {
            "AccountCreate": {
                "id": 1,
                "TopicPartition": 2,
                "Mongo": True,
                "Splunk": True,
                "ELK": False,
                "BigQuery": True
            },
            "DeviceCreate": {
                "id": 2,
                "TopicPartition": 2,
                "Mongo": True,
                "Splunk": False,
                "ELK": True,
                "BigQuery": True
            },
            "SessionActive": {
                "id": 3,
                "TopicPartition": 2,
                "Mongo": True,
                "Splunk": True,
                "ELK": True,
                "BigQuery": True
            },
        }

    def test_account_create_must_send_to_mongo_splunk_bq(self):
        bq_logger, elk_logger, logger_manager, mongo_logger, splunk_logger = self.set_up_logger_manager()

        logger_manager.send('AccountCreate', {}, time.time())
        self.assertEqual(mongo_logger.send.call_count, 1)
        self.assertEqual(splunk_logger.send.call_count, 1)
        self.assertEqual(elk_logger.send.call_count, 0)
        self.assertEqual(bq_logger.send.call_count, 1)

    def test_device_create_must_send_to_mongo_elk_bq(self):
        bq_logger, elk_logger, logger_manager, mongo_logger, splunk_logger = self.set_up_logger_manager()

        logger_manager.send('DeviceCreate', {}, time.time())
        self.assertEqual(mongo_logger.send.call_count, 1)
        self.assertEqual(splunk_logger.send.call_count, 0)
        self.assertEqual(elk_logger.send.call_count, 1)
        self.assertEqual(bq_logger.send.call_count, 1)

    def test_session_active_topic(self):
        bq_logger, elk_logger, logger_manager, mongo_logger, splunk_logger = self.set_up_logger_manager()

        logger_manager.send('SessionActive', {}, time.time())
        self.assertEqual(splunk_logger.send.call_args_list[0].kwargs['topic'], 'dev_yak_splunk_session_active')
        self.assertEqual(elk_logger.send.call_args_list[0].kwargs['topic'], 'dev_yak_elk_session_active')
        self.assertEqual(bq_logger.send.call_args_list[0].kwargs['topic'], 'dev_yak_bigquery_session_active')

    def set_up_logger_manager(self):
        logger_manager = ArkDataLoggerManager(self.config, env='dev', code_name='yak')
        mongo_logger = Mock(spec=MongoLogger)
        splunk_logger = Mock(spec=KafkaLogger)
        elk_logger = Mock(spec=KafkaLogger)
        bq_logger = Mock(spec=KafkaLogger)
        logger_manager.register('Mongo', mongo_logger)
        logger_manager.register('Splunk', splunk_logger)
        logger_manager.register('ELK', elk_logger)
        logger_manager.register('BigQuery', bq_logger)
        return bq_logger, elk_logger, logger_manager, mongo_logger, splunk_logger


if __name__ == '__main__':
    unittest.main()
