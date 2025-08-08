# 數據傳輸核心模組使用說明

## ArkDataLoggerManager
### 設定檔
1. Kafka、Mongo 連線設定
   + 預設位置：{package}/Game/config/{dev, test, release}/cdp.cfg
   + 設定檔範例
   ```config
   [Init]
   check_attributes = true
   large_numbers = false
   
   [Kafka]
   enable = true
   topic = TestKafka_yak
   isInternal = false
   bootstrap_servers = 127.0.0.1:9092
   
   [KafkaErrorLog]
   MongoHost = 127.0.0.1
   MongoPort = 27017
   DatabaseName = yak
   LoggerName=YakTestErrorLog
   CollName=KafkaErrorLog
   
   [MongoErrorLog]
   MongoHost = 127.0.0.1
   MongoPort = 27017
   DatabaseName = yak
   LoggerName=YakTestMongoErrorLog
   CollName=MongoErrorLog
   
   [ArkCdpLog]
   MongoHost = 127.0.0.1
   MongoPort = 27017
   DatabaseName = yak_backend
   LoggerName=YAK_BACKEND_LOG
   CollName=YAK_BACKEND_LOG
   MongoPoolSize = 300
   ```
2. 資料傳輸模組設定
   + 預設位置：{package}/Game/config/{dev, test, release}/cdp_trans.json
   + 設定檔範例
   ```json
   {
     "AccountCreate": {
       "KafkaTopicPartition": 2,
       "Mongo": true,
       "Splunk": true,
       "ELK": false,
       "BigQuery": true
     },
     "DeviceCreate": {
       "KafkaTopicPartition": 2,
       "Mongo": true,
       "Splunk": true,
       "ELK": false,
       "BigQuery": true
       },
     "SessionActive": {
       "KafkaTopicPartition": 2,
       "Mongo": true,
       "Splunk": true,
       "ELK": false,
       "BigQuery": true
     }
   }
   ```
### 使用步驟
1. 初始化 Data Logger
    ```python
    from ArkCdp.data_logger import ark_data_logger_generator
    
    
    def ark_data_logger_init(self, error_logger_config_path):
        # 初始化 ArkDataLogger (設定連線資訊、例外錯誤紀錄方式)
        logger_cfg_path = self.getArkPath('Game') + "/config/" + self.version + "/data_log.json"  # Kafka 連線資訊設定檔
        logger_cfg_key = 'yak_test_log'
        error_logger_section = 'YakKafkaErrorLog'
        return ark_data_logger_generator.get_ark_data_logger(logger_cfg_path, logger_cfg_key, error_logger_config_path,
                                                             error_logger_section, self.version, self.codeName,
                                                             self.logger.game_log())
    
    def mongo_backend_logger_init(self, config_path):
        return ark_data_logger_generator.get_mongo_backend_logger(config_path, 'YakBackendLog', self.logger.game_log())
    
    
    mongo_logger_config_path = self.getArkPath('Game') + "/config/" + self.version + '/mongo_logger.cfg'
    self.data_logger = self.ark_data_logger_init(mongo_logger_config_path)
    self.mongo_backend_logger = self.mongo_backend_logger_init(mongo_logger_config_path)
    ```
2. 初始化 ArkDataLoggerManager
    ```python
    trans_config_path = self.getArkPath('Game') + "/config/" + self.version + "/trans.json"
    config = get_json_config(trans_config_path)
    ark_data_manager = ArkDataLoggerManager(config, self.version, self.codeName)
    ark_data_manager.register('ELK', self.data_logger)
    ark_data_manager.register('Splunk', self.data_logger)
    ark_data_manager.register('BigQuery', self.data_logger)
    ark_data_manager.register('Mongo', self.mongo_backend_logger)
    ```
3. 傳送 Log
    ```python
    ark_data_manager.send('AccountCreate', {'Test': 123}, 1278276905502)
    ```

## Logging Handler (Server System Log)
### 使用方式

```python
from ArkCdp.data_logger.ark_logging_handler import ArkLoggingHandler


def set_ark_logger_handler(self, code_name, env, kafka_logger):
   handler = ArkLoggingHandler(code_name, env, kafka_logger)
   self.logger.game_log().addHandler(handler)
   self.logger.sys_log().addHandler(handler)


self.set_ark_logger_handler(code_name, version, self.data_logger)
```