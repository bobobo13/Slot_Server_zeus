import unittest

from mock import Mock

from ..libs.kafka_producer import KafkaProducer
from ..modules.kafka_logger import KafkaLogger


class KafkaLoggerTests(unittest.TestCase):
    def setUp(self):
        self.config = {
            "AccountCreate": {
                "id": 1,
                "TopicPartition": 2,
                "Mongo": True,
                "Splunk": True,
                "ELK": False,
                "BigQuery": True
            }
        }

    def test_when_dev_yak_account_create_topic_must_be_dev_yak_splunk_account_create(self):
        producer = Mock(spec=KafkaProducer)
        kafka_logger = KafkaLogger(producer, 'topic', None, topic_prefix='dev_yak')
        kafka_logger.setup('Splunk', self.config)
        self.assertEqual(producer.set_topic_partitions.call_args_list[0].args[0], 'dev_yak_splunk_account_create')


if __name__ == '__main__':
    unittest.main()
