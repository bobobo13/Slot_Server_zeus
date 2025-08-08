# 數據傳輸核心模組使用說明

## ArkCdp
### 安裝套件
```python
sudo pip install kafka-python==2.0.2
sudo pip install kazoo==2.8.0
```
### 設定檔
1.Kafka、Mongo 連線設定
   + 預設位置：{package}/Game/config/{dev, test, release}/cdp.cfg
   + 設定檔範例
   ```config
   [Init]
   check_attributes = 是否要啟用數值型別檢查 (預設為true)
   large_numbers = 是否要將大數欄位轉成Str (預設為false)
   
   [Kafka]
   enable = 是否要啟用Kafka (預設為true)
   topic = 預設Topic名稱 (預設為Kafka_mycodename)
   isInternal = 是否使用內部IP連線  (預設為false)
   bootstrap_servers = 跟系統/維運組確認Kafka連線位置 (IP1:Port,IP2:Port,IP3:Port)
   
   [KafkaErrorLog]
   MongoHost = 127.0.0.1
   MongoPort = 27017
   DatabaseName = mycodename_kafka  Kafka傳送失敗時，儲存位置
   LoggerName = KafkaErrorLog
   CollName = KafkaErrorLog
   MongoUser = (選填)若MongoDB無權限設定，則不要填
   MongoPwd = (選填)若MongoDB無權限設定，則不要填
   
   [MongoErrorLog]
   MongoHost = 127.0.0.1
   MongoPort = 27017
   DatabaseName = mycodename_kafka  此為MongoDB備援機，當Log無法寫入ArkCdpLog時，會寫到這台
   LoggerName = MongoErrorLog
   CollName = MongoErrorLog
   MongoUser = (選填)若MongoDB無權限設定，則不要填
   MongoPwd = (選填)若MongoDB無權限設定，則不要填
   
   [ArkCdpLog]
   MongoHost = 127.0.0.1
   MongoPort = 27017
   DatabaseName = mycodename_cdp  所有Log都會進行備份，可在cdp_trans.josn做調整
   LoggerName = ArkCdpLog
   CollName = ArkCdpLog
   MongoUser = (選填)若MongoDB無權限設定，則不要填
   MongoPwd = (選填)若MongoDB無權限設定，則不要填
   ```
2.資料傳輸模組-標準Log設定
   + 預設位置：{package}/Game/config/{dev, test, release}/cdp_trans.json
   + 設定檔內容 {true=要傳送, false=不傳送}
   ```json
    {
        "AccountCreate": {
            "KafkaTopicPartition": 6,
            "Mongo": true,
            "Splunk": true,
            "ELK": false,
            "BigQuery": true
        },
        "DeviceCreate": {
            "KafkaTopicPartition": 6,
            "Mongo": true,
            "Splunk": true,
            "ELK": false,
            "BigQuery": true
        },
        "SessionActive": {
            "KafkaTopicPartition": 6,
            "Mongo": true,
            "Splunk": true,
            "ELK": false,
            "BigQuery": true
        },
        "SessionLength": {
            "KafkaTopicPartition": 6,
            "Mongo": true,
            "Splunk": true,
            "ELK": false,
            "BigQuery": true
        },
        "GameConsume": {
            "KafkaTopicPartition": 6,
            "Mongo": true,
            "Splunk": true,
            "ELK": false,
            "BigQuery": true
        },
        "GameConsumeFailed": {
            "KafkaTopicPartition": 6,
            "Mongo": true,
            "Splunk": false,
            "ELK": true,
            "BigQuery": true
        },
        "GameConsumeGetCoin": {
            "KafkaTopicPartition": 6,
            "Mongo": true,
            "Splunk": true,
            "ELK": false,
            "BigQuery": true
        },
        "GameConsumeGetItem": {
            "KafkaTopicPartition": 6,
            "Mongo": true,
            "Splunk": true,
            "ELK": false,
            "BigQuery": true
        },
        "DeliverCoin": {
            "KafkaTopicPartition": 6,
            "Mongo": true,
            "Splunk": false,
            "ELK": false,
            "BigQuery": true
        },
        "DeliverItem": {
            "KafkaTopicPartition": 6,
            "Mongo": true,
            "Splunk": false,
            "ELK": false,
            "BigQuery": true
        },
        "DetailCoin": {
            "KafkaTopicPartition": 6,
            "Mongo": true,
            "Splunk": false,
            "ELK": false,
            "BigQuery": true
        },
        "DetailItem": {
            "KafkaTopicPartition": 6,
            "Mongo": true,
            "Splunk": false,
            "ELK": false,
            "BigQuery": true
        },
        "AssignAwardCoin": {
            "KafkaTopicPartition": 6,
            "Mongo": true,
            "Splunk": true,
            "ELK": false,
            "BigQuery": true
        },
        "AssignAwardItem": {
            "KafkaTopicPartition": 6,
            "Mongo": true,
            "Splunk": true,
            "ELK": false,
            "BigQuery": true
        },
        "TransferCoin": {
            "KafkaTopicPartition": 6,
            "Mongo": true,
            "Splunk": true,
            "ELK": false,
            "BigQuery": true
        },
        "TransferItem": {
            "KafkaTopicPartition": 6,
            "Mongo": true,
            "Splunk": true,
            "ELK": false,
            "BigQuery": true
        },
        "DetailBetWin": {
            "KafkaTopicPartition": 6,
            "Mongo": true,
            "Splunk": false,
            "ELK": true,
            "BigQuery": true
        },
        "PlayerProfile": {
            "KafkaTopicPartition": 6,
            "Mongo": true,
            "Splunk": true,
            "ELK": false,
            "BigQuery": true
        },
        "HourlyStats": {
            "KafkaTopicPartition": 6,
            "Mongo": true,
            "Splunk": false,
            "ELK": false,
            "BigQuery": true
        },
        "PermissionChange": {
            "KafkaTopicPartition": 6,
            "Mongo": true,
            "Splunk": true,
            "ELK": false,
            "BigQuery": true
        },
        "ClientErrorLog": {
            "KafkaTopicPartition": 6,
            "Mongo": true,
            "Splunk": false,
            "ELK": true,
            "BigQuery": false
        },
        "ArkSysLog": {
            "KafkaTopicPartition": 6,
            "Mongo": false,
            "Splunk": false,
            "ELK": true,
            "BigQuery": false
        }
    }
   ```
   + Kafka 資料預設儲存 7 天，若需要針對特定 Topic 調整數據過期時間，請加入 KafkaTopicConfig 設定，設定的參數為 `retention.ms`，單位為毫秒 (ex: 86400000 為一天)
       + 以下為將 ArkSysLog 調整為只存在 Kafka 一天的 `cdp_trans.json` 修改範例
        ```json
        {
            ...
            "ArkSysLog": {
                "KafkaTopicPartition": 6,
                "Mongo": false,
                "Splunk": false,
                "ELK": true,
                "BigQuery": false,
                "KafkaTopicConfig": {"retention.ms": "86400000"}
            },
            ...
        }
        ```
3.資料傳輸模組-自定義Log設定
   + 預設位置：{package}/Game/config/{dev, test, release}/cdp_trans_custom.json
   + 設定檔內容 {true=要傳送, false=不傳送}
   ```json
    {
        "my_custom_event1": {
            "KafkaTopicPartition": 6,
            "Mongo": true,
            "Splunk": false,
            "ELK": true,
            "BigQuery": true
        }
    }
   ```
4.如果 cdp_trans.json 和 cdp_trans_custom.json 的Key重覆時，以 cdp_trans_custom.json 設定為主
### 使用步驟
1. 初始化 DataEvent
    ```python
    from ArkCdp.data_model.DataEvent import DataEvent
    
    cdp_cfg_path = os.path.join(self.getArkPath('Game'), 'config', self.version, 'cdp.cfg')
    code_name ="mycodename"
    self.data_event = DataEvent(cdp_cfg_path, self.logger.sys_log(), code_name, self.version, callback=self._kafka_cb)
    
    # 可以將Log訊息會寫到Kafka，再由Kafka寫到ELK。
    # Kafka的Topic名稱為 self.version + '_' + code_name + '_elk_ark_sys_log'
    data_event.set_ark_logger_handler(self.logger.game_log())
    data_event.set_ark_logger_handler(self.logger.sys_log())
    # self.logger.game_log().warn("可以將Log訊息寫到Kafka，再由Kafka寫到ELK")
    ```
2. 呼叫標準API，傳送標準Log
    ```python
    activedata = {"udid":"udid01", "sys_type":"ios", "sys_ver":"v1.23", "country":"tw", "curr_channel":"channel1",
                  "publish_ver":"v1.2", "lv":10, "vip_lv":2, "nickname":"暱稱"}
    r = self.data_event.activate_app(**activedata)
    if r is True:
       print 'activate_app: 驗證完成，資料以非同步傳送到kafka，可用callback得到傳送結果。\
              請定期檢查Log是否有warn、info訊息。'
    else:
       print 'activate_app: 驗證失敗，資料有誤，請檢查log是否有error訊息。'
    ```
3. 呼叫自定義函式，傳送自定義Log
    ```python
    from ArkCdp.data_model.data_model_erros import custom_event_not_exist
    try:
        # 1.使用自定義函式前，需要先到 cdp_trans_custom.json 添加設定

        # 取得Log內容
        my_custom_event1_dict = get_my_custom_event1()
   
        # 2.傳送自定義Log： 自定義Log名稱、Log內容、Log發生時間(以秒為單位的浮點數)
        r = data_event.send_custom_event('my_custom_event1', my_custom_event1_dict, time.time())
        
        if r is True:
           print '驗證完成，資料以非同步傳送到kafka，可用callback得到傳送結果。'
        else:
           print '驗證失敗，資料有誤，請檢查log內容。'
    except custom_event_not_exist:
        print '使用自定義Log前，需要先到cdp_trans_custom.json添加設定'
    ```
4. 自定義Log要寫到Big query，有哪些限制、要求?
    ```python
    # 01、不允許None，若None填預設值
    # 02、時間欄位用16位整數timestamp，若None填0。可呼叫共用函式 convert_timestamp
    # 03、日期欄位用str YYYY-MM-DD，若None填’1970-01-01’
    # 04、dict用json.dumps轉成str，若None填空字串
    # 05、dict用json.dumps轉成str時，若內容有非ASCII的文字在內時，使用ensure_ascii=False參數(e.g. json.dumps(DICT, ensure_ascii=False))
    # 06、list用json.dumps轉成str，list內的元素相同型態，若None填空字串
    # 07、list用json.dumps轉成str時，若內容有非ASCII的文字在內時，使用ensure_ascii=False參數(e.g. json.dumps(LIST, ensure_ascii=False))
    # 08、使用ensure_ascii=False參數時，dict、list內的字串需同樣為str或unicode
    # 09、字串不能有\n \r \t “雙引號 ,逗號。可呼叫共用函式 replace_special
    # 10、單一筆ROW不超過10MB
    ```
5. 共用函式
    ```python
    from ArkCdp.data_model.data_model_utilities import replace_special, convert_timestamp
    
    # convert_timestamp：將以秒為單位浮點數轉為整數16位，非int、long、float、Decimal會自動填0
    int16 = convert_timestamp(time.time())

    # replace_special：字串移除\n \r \t “雙引號 ,逗號
    operator = replace_special("操作\n\r\t員")
    nickname = replace_special(u"暱\"稱,")

    # 使用ensure_ascii=False時，字串需同樣為str或unicode
    str1 = replace_special(u"寶寶\n\r\t知道但寶寶不說")
    str2 = replace_special(u"哆啦a\"夢,")
    array =[{"array1":str1, "array2":str2, "array3":10000002}]
    dict = {"dict1":str1, "dict2":str2, "dict3":10000002}
    array_str = json.dumps(array, ensure_ascii=False)
    dict_str = json.dumps(dict, ensure_ascii=False)

    log_data = {"install_ts":int16, "user_id":10000001, "operator":operator, "nickname":nickname, "birthday":"2000-01-01",
         "coin_balance":"60000000000000000000000000000", "lv":3, "vip_lv":1, "array":array_str, "dict":dict_str}
    ```
## callback
### 使用方式 (請與維運組討論，有需要才接入 callback)
```python
def _kafka_cb(err, log_type, result, orig_data):
   print 'cb: ', log_type, err, result, orig_data
   if err is True:
      if log_type == 'MongoBackendLogger':
         """
         Parameters (result: AutoReconnect)
         ----------
             args : tuple
             details : list
             errors : list
             message : str
         """
         print log_type, result.message
      elif log_type == 'KafkaLogger':
         """
         Parameters
         ---------- 
             result: str
         """
         print log_type, result
   else:
      if log_type == 'MongoBackendLogger':
         """
         Parameters (result: pymongo.results.InsertOneResult)
         ----------
             acknowledged : bool
             inserted_id : str
         """
         r = {
            'acknowledged': result.acknowledged,
            'inserted_id': result.inserted_id,
         }
         print log_type, r
      elif log_type == 'KafkaLogger':
         """
         Parameters (result: RecordMetadata)
         ----------
             topic : str,
             partition : number,
             topic_partition : TopicPartition (topic='test_mycodename_bigquery_session_active', partition=1),
             offset : number,
             timestamp : number(13),
             log_start_offset : number,
             checksum : None,
             serialized_key_size : number,
             serialized_value_size : number,
             serialized_header_size : number
         """
         r = {
            'topic': result.topic,
            'partition': result.partition,
            'topic_partition': result.topic_partition,
            'offset': result.offset,
            'timestamp': result.timestamp,
            'log_start_offset': result.log_start_offset,
            'checksum': result.checksum,
            'serialized_key_size': result.serialized_key_size,
            'serialized_value_size': result.serialized_value_size,
            'serialized_header_size': result.serialized_header_size,
         }
         print log_type, r
```
## 測試
### 使用方式
1.到 data_model\tests\config\dev\cdp.cfg修改kafka和MongoDB的連線設定

2.到ArkCdp資料夾上一層 執行 python -m ArkCdp.data_model.tests.DataEventTest，可將測試資料傳送至Kafka和MongoDB (ArkCdp名稱可自行取代為自己clone下來的資料夾名稱)