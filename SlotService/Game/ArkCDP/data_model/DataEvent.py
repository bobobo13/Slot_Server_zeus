# -*- coding: utf-8 -*-
import datetime
import time
import six
if six.PY3:
    import configparser as ConfigParser
else:
    import ConfigParser
from ..data_logger import ark_data_logger_generator
from ..data_logger.ark_data_logger_manager import ArkDataLoggerManager
from ..data_logger.ark_logging_handler import ArkLoggingHandler
from .data_model_utilities import timestamp_valid, timestamp_valid_base, float_round
from .data_model_utilities import int_valid, float_valid, int_and_float_valid_base, longitude_valid, latitude_valid
from .data_model_utilities import date_valid, positive_valid, negative_valid
from .data_model_utilities import str_valid, str_valid_base, convert_large_numbers
from .data_model_erros import custom_event_not_exist


class DataEvent(object):
    GTM_TIME_ZONE = 8
    GTM_TIME_ZONE_SEC = 28800

    def __init__(self, cdp_cfg_path, logger, code_name='yak', version='dev', callback=None,
                 cdp_trans_path=None, cdp_trans_custom_path=None, is_pro_date_now=False):
        self.cdp_cfg_path = cdp_cfg_path
        self.code_name = code_name
        self.version = version
        self.callback = callback
        self.logger = logger

        self.data_logger = self.ark_data_logger_init()
        self.mongo_backend_logger = self.mongo_backend_logger_init()

        cdp_trans1_path = self.cdp_cfg_path.replace('cdp.cfg', 'cdp_trans.json') \
            if cdp_trans_path is None else cdp_trans_path
        cdp_trans2_path = self.cdp_cfg_path.replace('cdp.cfg', 'cdp_trans_custom.json') \
            if cdp_trans_custom_path is None else cdp_trans_custom_path
        cdp_trans1 = ark_data_logger_generator.get_json_config(cdp_trans1_path)
        cdp_custom_trans = ark_data_logger_generator.get_json_config(cdp_trans2_path)

        for key in cdp_custom_trans.keys():
            cdp_custom_trans[key]['IsCustom'] = True

        cdp_trans = cdp_trans1.copy()
        cdp_trans.update(cdp_custom_trans)
        self.cdp_trans = cdp_trans

        self.ark_data_manager = ArkDataLoggerManager(self.cdp_trans, self.version, self.code_name)
        self.ark_data_manager.register('ELK', self.data_logger)
        self.ark_data_manager.register('Splunk', self.data_logger)
        self.ark_data_manager.register('BigQuery', self.data_logger)
        self.ark_data_manager.register('Mongo', self.mongo_backend_logger)

        cdp_config = ConfigParser.RawConfigParser()
        cdp_config.read(self.cdp_cfg_path)
        self.check_attributes = cdp_config.getboolean('Init', 'check_attributes')
        self.large_numbers = cdp_config.getboolean('Init', 'large_numbers')

        self.custom_event_check_log = {}
        self.custom_event_callback = {}
        self.is_pro_date_now = is_pro_date_now

    def ark_data_logger_init(self):
        # 初始化 ArkDataLogger (設定連線資訊、例外錯誤紀錄方式)
        kafka_cfg_section = 'Kafka'
        error_logger_section = 'KafkaErrorLog'
        return ark_data_logger_generator.get_ark_data_logger(self.cdp_cfg_path, kafka_cfg_section,
                                                             error_logger_section, self.version, self.code_name,
                                                             self.logger)

    def mongo_backend_logger_init(self):
        backend_cfg_section = 'ArkCdpLog'
        error_logger_section = 'MongoErrorLog'
        return ark_data_logger_generator.get_mongo_backend_logger(self.cdp_cfg_path, backend_cfg_section, self.logger,
                                                                  error_log_section=error_logger_section,
                                                                  gmt_time_zone=self.GTM_TIME_ZONE)

    def set_ark_logger_handler(self, logger_obj):
        # 需要儘量在程式的開頭註冊 Handler
        handler = ArkLoggingHandler(self.code_name, self.version, self.data_logger)
        # 各專案如果有自己定義的 logger 也需要透過 addHandler 設定轉拋至 Kafka
        logger_obj.addHandler(handler)

    def get_logger_datetime(self, event_ts=None):
        if self.is_pro_date_now or event_ts is None:
            dt = self.get_ts_datetime()
        else:
            dt = self.get_ts_datetime(self.ark_data_manager.format_ts(event_ts, 10))
        return dt

    def get_ts_datetime(self, timestamp=None):
        if timestamp is None:
            return datetime.datetime.utcnow() + datetime.timedelta(hours=self.GTM_TIME_ZONE)
        return datetime.datetime.utcfromtimestamp(timestamp) + datetime.timedelta(hours=self.GTM_TIME_ZONE)

    @staticmethod
    def get_dt_pro_date(datetime_form):
        return datetime_form.strftime("%Y-%m-%d")

    @staticmethod
    def get_dt_mongo_coll_date(datetime_form):
        return datetime_form.strftime("%Y%m%d")

    def get_pro_date(self):
        tm = time.gmtime(time.time() + self.GTM_TIME_ZONE_SEC)
        return time.strftime("%Y-%m-%d", tm)

    # 1.開啟遊戲App：SessionActive
    def activate_app(self, udid, sys_type, sys_ver, country, curr_channel, publish_ver, lv, vip_lv,
                     appsflyer_id=None, region=None, city=None, role_id=None, role_nickname=None, dev=None,
                     mac=None, android_id=None, aaid=None, imei=None, idfa=None, idfv=None, fid=None, ip=None,
                     network=None, longitude=None, latitude=None, user_id=None, nickname=None, channel=None,
                     install_source=None, logo_id=None, logo_name=None, kiosk_id=None, kiosk_name=None, time_zone=None,
                     lang=None, custom_data=None, activate_ts=None):
        """
        :param str udid: 裝置ID
        :param str sys_type: 操作系統
        :param str sys_ver: 操作系統版本
        :param str country: 所在地區(國別)
        :param str curr_channel: 玩家登入渠道
        :param str publish_ver: 遊戲版本
        :param int lv: 目前等級
        :param int vip_lv: 目前VIP等級
        :param str appsflyer_id: 媒體下載歸因裝置ID, 允許None
        :param str region: 所在地區省州,允許None
        :param str city: 所在地區城市,允許None
        :param str role_id: 玩家角色ID,允許None
        :param str role_nickname: 玩家角色暱稱, 允許None
        :param str dev: 機型,允許None
        :param str mac: 裝置MAC,允許None
        :param str android_id: 允許None
        :param str aaid: 允許None
        :param str imei: 允許None
        :param str idfa: 允許None
        :param str idfv: 允許None
        :param str fid: 允許None
        :param str ip: 玩家登入IP,允許None,
        :param str network: 聯網方式,允許None
        :param float longitude: 經度,允許None
        :param float latitude: 緯度,允許None
        :param int user_id: 玩家ID帳號,允許None
        :param str nickname: 暱稱,允許None
        :param str channel: 下載平台渠道,允許None
        :param str install_source: 第三方媒體首次安裝來源, 允許None
        :param int logo_id: 代理商ID, 允許None
        :param str logo_name: 允許None
        :param int kiosk_id: 店家ID, 允許None
        :param str kiosk_name: 允許None
        :param float time_zone: 玩家時區,允許None
        :param str lang: 使用語系,允許None
        :param str custom_data: 自訂擴充定義與值,允許None
        :param float activate_ts: 開啟遊戲時間time.time(), 允許None, 若None系統會用現在時間
        """
        fun_name = 'activate_app'

        udid_err, udid_result = str_valid(fun_name, 'udid', udid, '', allow_none=False)
        if udid_err is not None:
            self.logger.error(udid_err + ' and required')
            return False

        activate_err, activate_ts_result = timestamp_valid(fun_name, 'activate_ts', activate_ts, allow_none=True)
        if activate_err is not None:
            self.logger.info(activate_err)

        if self.check_attributes:
            sys_type_err, sys_type_result = str_valid(fun_name, 'sys_type', sys_type, '', allow_none=False)
            if sys_type_err is not None:
                self.logger.warn(sys_type_err)

            sys_ver_err, sys_ver_result = str_valid(fun_name, 'sys_ver', sys_ver, '', allow_none=False)
            if sys_ver_err is not None:
                self.logger.warn(sys_ver_err)

            country_err, country_result = str_valid(fun_name, 'country', country, '', allow_none=False)
            if country_err is not None:
                self.logger.warn(country_err)

            curr_channel_err, curr_channel_result = str_valid(fun_name, 'curr_channel', curr_channel, '',
                                                              allow_none=False)
            if curr_channel_err is not None:
                self.logger.warn(curr_channel_err)

            publish_ver_err, publish_ver_result = str_valid(fun_name, 'publish_ver', publish_ver, '', allow_none=False)
            if publish_ver_err is not None:
                self.logger.warn(publish_ver_err)

            lv_err, lv_result = int_valid(fun_name, 'lv', lv, 0, allow_none=False)
            if lv_err is not None:
                self.logger.warn(lv_err)

            vip_lv_err, vip_lv_result = int_valid(fun_name, 'vip_lv', vip_lv, 0, allow_none=False)
            if vip_lv_err is not None:
                self.logger.warn(vip_lv_err)

            longitude_err, longitude_result = longitude_valid(fun_name, 'longitude', longitude, -999, allow_none=True)
            if longitude_err is not None:
                self.logger.info(longitude_err)

            latitude_err, latitude_result = latitude_valid(fun_name, 'latitude', latitude, -999, allow_none=True)
            if latitude_err is not None:
                self.logger.info(latitude_err)

            user_id_err, user_id_result = int_valid(fun_name, 'user_id', user_id, 0, allow_none=True)
            if user_id_err is not None:
                self.logger.info(user_id_err)

            logo_id_err, logo_id_result = int_valid(fun_name, 'logo_id', logo_id, 0, allow_none=True)
            if logo_id_err is not None:
                self.logger.info(logo_id_err)

            kiosk_id_err, kiosk_id_result = int_valid(fun_name, 'kiosk_id', kiosk_id, 0, allow_none=True)
            if kiosk_id_err is not None:
                self.logger.info(kiosk_id_err)

            time_zone_err, time_zone_result = float_valid(fun_name, 'time_zone', time_zone, -999, allow_none=True)
            if time_zone_err is not None:
                self.logger.info(time_zone_err)
        else:
            sys_type_result = str_valid_base(sys_type, '')
            sys_ver_result = str_valid_base(sys_ver, '')
            country_result = str_valid_base(country, '')
            curr_channel_result = str_valid_base(curr_channel, '')
            publish_ver_result = str_valid_base(publish_ver, '')
            lv_result = 0 if lv is None else lv
            vip_lv_result = 0 if vip_lv is None else vip_lv
            longitude_result = -999 if longitude is None else longitude
            latitude_result = -999 if latitude is None else latitude
            user_id_result = 0 if user_id is None else user_id
            logo_id_result = 0 if logo_id is None else logo_id
            kiosk_id_result = 0 if kiosk_id is None else kiosk_id
            time_zone_result = -999 if time_zone is None else time_zone
        dt = self.get_logger_datetime(activate_ts_result)
        sa = {
            'ProDate': self.get_dt_pro_date(dt),
            'ActiveTs': activate_ts_result,
            'UserID': user_id_result,
            'Nickname': str_valid_base(nickname, '', replace_special_word=True),
            'UDID': udid_result,
            'AppsflyerID': str_valid_base(appsflyer_id, ''),
            'SysType': sys_type_result,
            'SysVer': sys_ver_result,
            'Country': country_result,
            'Region': str_valid_base(region, ''),
            'City': str_valid_base(city, ''),
            'Channel': str_valid_base(channel, ''),
            'CurrChannel': curr_channel_result,
            'InstallSource': str_valid_base(install_source, ''),
            'PublishVer': publish_ver_result,
            'LV': lv_result,
            'VipLV': vip_lv_result,
            'LogoID': logo_id_result,
            'LogoName': str_valid_base(logo_name, ''),
            'KioskID': kiosk_id_result,
            'KioskName': str_valid_base(kiosk_name, ''),
            'RoleID': str_valid_base(role_id, ''),
            'RoleNickname': str_valid_base(role_nickname, '', replace_special_word=True),
            'DEV': str_valid_base(dev, ''),
            'MAC': str_valid_base(mac, ''),
            'AndroidID': str_valid_base(android_id, ''),
            'AAID': str_valid_base(aaid, ''),
            'IMEI': str_valid_base(imei, ''),
            'IDFA': str_valid_base(idfa, ''),
            'IDFV': str_valid_base(idfv, ''),
            'FID': str_valid_base(fid, ''),
            'IP': str_valid_base(ip, ''),
            'Network': str_valid_base(network, ''),
            'Longitude': longitude_result,
            'Latitude': latitude_result,
            'TimeZone': time_zone_result,
            'Lang': str_valid_base(lang, ''),
            'CustomData': str_valid_base(custom_data, '', replace_special_word=False),
        }
        self.ark_data_manager.send("SessionActive", sa, activate_ts_result, callback=self.callback,
                                   mongo_coll_date=self.get_dt_mongo_coll_date(dt))
        return True

    # 2.登入遊戲：SessionActive
    def login(self, udid, user_id, sys_type, sys_ver, country, curr_channel, publish_ver, lv, vip_lv, appsflyer_id=None,
              region=None, city=None, role_id=None, role_nickname=None, dev=None, mac=None, android_id=None, aaid=None,
              imei=None, idfa=None, idfv=None, fid=None, ip=None, network=None, longitude=None, latitude=None,
              nickname=None, channel=None, install_source=None, logo_id=None, logo_name=None, kiosk_id=None,
              kiosk_name=None, time_zone=None, lang=None, custom_data=None, login_ts=None):
        """
        :param str udid: 裝置ID
        :param int user_id: 玩家ID帳號
        :param str sys_type: 操作系統
        :param str sys_ver: 操作系統版本
        :param str country: 所在地區(國別)
        :param str curr_channel: 下載平台渠道
        :param str publish_ver: 遊戲版本
        :param int lv: 目前等級
        :param int vip_lv: 目前VIP等級
        :param str appsflyer_id: 媒體下載歸因裝置ID, 允許None
        :param str region: 所在地區省州, 允許None
        :param str city: 所在地區城市, 允許None
        :param str role_id: 玩家角色ID, 允許None
        :param str role_nickname: 玩家角色暱稱, 允許None
        :param str dev: 機型,允許None
        :param str mac: 裝置MAC,允許None
        :param str android_id: 允許None
        :param str aaid: 允許None
        :param str imei: 允許None
        :param str idfa: 允許None
        :param str idfv: 允許None
        :param str fid: 允許None
        :param str ip: 玩家登入IP, 允許None,
        :param str network: 聯網方式, 允許None
        :param float longitude: 經度, 允許None
        :param float latitude: 緯度, 允許None
        :param str nickname: 暱稱, 允許None
        :param str channel: 下載平台渠道, 允許None
        :param str install_source: 第三方媒體首次安裝來源, 允許None
        :param int logo_id: 代理商ID, 允許None
        :param str logo_name: 允許None
        :param int kiosk_id: 店家ID, 允許None
        :param str kiosk_name: 允許None
        :param float time_zone: 玩家時區, 允許None
        :param str lang: 使用語系, 允許None
        :param str custom_data: 自訂擴充定義與值,允許None
        :param float login_ts: 登入遊戲時間time.time(), 允許None, 若None系統會用現在時間
        """
        fun_name = 'login'

        udid_err, udid_result = str_valid(fun_name, 'udid', udid, '', allow_none=False)
        if udid_err is not None:
            self.logger.error(udid_err + ' and required')
            return False

        login_err, login_ts_result = timestamp_valid(fun_name, 'login_ts', login_ts, allow_none=True)
        if login_err is not None:
            self.logger.info(login_err)

        if self.check_attributes:
            user_id_err, user_id_result = int_valid(fun_name, 'user_id', user_id, 0, allow_none=False)
            if user_id_err is not None:
                self.logger.warn(user_id_err)

            sys_type_err, sys_type_result = str_valid(fun_name, 'sys_type', sys_type, '', allow_none=False)
            if sys_type_err is not None:
                self.logger.warn(sys_type_err)

            sys_ver_err, sys_ver_result = str_valid(fun_name, 'sys_ver', sys_ver, '', allow_none=False)
            if sys_ver_err is not None:
                self.logger.warn(sys_ver_err)

            country_err, country_result = str_valid(fun_name, 'country', country, '', allow_none=False)
            if country_err is not None:
                self.logger.warn(country_err)

            curr_channel_err, curr_channel_result = str_valid(fun_name, 'curr_channel', curr_channel, '',
                                                              allow_none=False)
            if curr_channel_err is not None:
                self.logger.warn(curr_channel_err)

            publish_ver_err, publish_ver_result = str_valid(fun_name, 'publish_ver', publish_ver, '', allow_none=False)
            if publish_ver_err is not None:
                self.logger.warn(publish_ver_err)

            lv_err, lv_result = int_valid(fun_name, 'lv', lv, 0, allow_none=False)
            if lv_err is not None:
                self.logger.warn(lv_err)

            vip_lv_err, vip_lv_result = int_valid(fun_name, 'vip_lv', vip_lv, 0, allow_none=False)
            if vip_lv_err is not None:
                self.logger.warn(vip_lv_err)

            longitude_err, longitude_result = longitude_valid(fun_name, 'longitude', longitude, -999, allow_none=True)
            if longitude_err is not None:
                self.logger.info(longitude_err)

            latitude_err, latitude_result = latitude_valid(fun_name, 'latitude', latitude, -999, allow_none=True)
            if latitude_err is not None:
                self.logger.info(latitude_err)

            logo_id_err, logo_id_result = int_valid(fun_name, 'logo_id', logo_id, 0, allow_none=True)
            if logo_id_err is not None:
                self.logger.info(logo_id_err)

            kiosk_id_err, kiosk_id_result = int_valid(fun_name, 'kiosk_id', kiosk_id, 0, allow_none=True)
            if kiosk_id_err is not None:
                self.logger.info(kiosk_id_err)

            time_zone_err, time_zone_result = float_valid(fun_name, 'time_zone', time_zone, -999, allow_none=True)
            if time_zone_err is not None:
                self.logger.info(time_zone_err)
        else:
            user_id_result = 0 if user_id is None else user_id
            sys_type_result = str_valid_base(sys_type, '')
            sys_ver_result = str_valid_base(sys_ver, '')
            country_result = str_valid_base(country, '')
            curr_channel_result = str_valid_base(curr_channel, '')
            publish_ver_result = str_valid_base(publish_ver, '')
            lv_result = 0 if lv is None else lv
            vip_lv_result = 0 if vip_lv is None else vip_lv
            longitude_result = -999 if longitude is None else longitude
            latitude_result = -999 if latitude is None else latitude
            logo_id_result = 0 if logo_id is None else logo_id
            kiosk_id_result = 0 if kiosk_id is None else kiosk_id
            time_zone_result = -999 if time_zone is None else time_zone
        dt = self.get_logger_datetime(login_ts_result)
        sa = {
            'ProDate': self.get_dt_pro_date(dt),
            'ActiveTs': login_ts_result,
            'UserID': user_id_result,
            'Nickname': str_valid_base(nickname, '', replace_special_word=True),
            'UDID': udid_result,
            'AppsflyerID': str_valid_base(appsflyer_id, ''),
            'SysType': sys_type_result,
            'SysVer': sys_ver_result,
            'Country': country_result,
            'Region': str_valid_base(region, ''),
            'City': str_valid_base(city, ''),
            'Channel': str_valid_base(channel, ''),
            'CurrChannel': curr_channel_result,
            'InstallSource': str_valid_base(install_source, ''),
            'PublishVer': publish_ver_result,
            'LV': lv_result,
            'VipLV': vip_lv_result,
            'LogoID': logo_id_result,
            'LogoName': str_valid_base(logo_name, ''),
            'KioskID': kiosk_id_result,
            'KioskName': str_valid_base(kiosk_name, ''),
            'RoleID': str_valid_base(role_id, ''),
            'RoleNickname': str_valid_base(role_nickname, '', replace_special_word=True),
            'DEV': str_valid_base(dev, ''),
            'MAC': str_valid_base(mac, ''),
            'AndroidID': str_valid_base(android_id, ''),
            'AAID': str_valid_base(aaid, ''),
            'IMEI': str_valid_base(imei, ''),
            'IDFA': str_valid_base(idfa, ''),
            'IDFV': str_valid_base(idfv, ''),
            'FID': str_valid_base(fid, ''),
            'IP': str_valid_base(ip, ''),
            'Network': str_valid_base(network, ''),
            'Longitude': longitude_result,
            'Latitude': latitude_result,
            'TimeZone': time_zone_result,
            'Lang': str_valid_base(lang, ''),
            'CustomData': str_valid_base(custom_data, '', replace_special_word=False),
        }
        self.ark_data_manager.send("SessionActive", sa, login_ts_result, callback=self.callback,
                                   mongo_coll_date=self.get_dt_mongo_coll_date(dt))
        return True

    # 3.綁定新裝置：DeviceCreate
    def bind_device(self, udid, user_id, sys_type, sys_ver, country, curr_channel, publish_ver,
                    appsflyer_id=None, region=None, city=None, role_id=None, role_nickname=None, dev=None, mac=None,
                    android_id=None, aaid=None, imei=None, idfa=None, idfv=None, fid=None,
                    nickname=None, channel=None, install_source=None, install_ts=None, logo_id=None, logo_name=None,
                    kiosk_id=None, kiosk_name=None, custom_data=None, device_create_ts=None):
        """
        :param str udid: 裝置ID
        :param int user_id: 玩家ID帳號
        :param str sys_type: 操作系統
        :param str sys_ver: 操作系統版本
        :param str country: 所在地區(國別)
        :param str curr_channel: 下載平台渠道
        :param str publish_ver: 遊戲版本
        :param str appsflyer_id: 媒體下載歸因裝置ID, 允許None
        :param str region: 所在地區省州, 允許None
        :param str city: 所在地區城市, 允許None
        :param str role_id: 玩家角色ID, 允許None
        :param str role_nickname: 玩家角色暱稱, 允許None
        :param str dev: 機型, 允許None
        :param str mac: 裝置MAC, 允許None
        :param str android_id: 允許None
        :param str aaid: 允許None
        :param str imei: 允許None
        :param str idfa: 允許None
        :param str idfv: 允許None
        :param str fid: 允許None
        :param str nickname: 暱稱, 允許None
        :param str channel: 下載平台渠道, 允許None
        :param str install_source: 第三方媒體首次安裝來源, 允許None
        :param float install_ts: 第三方媒體首次安裝時間time.time(), 允許None(0)
        :param int logo_id: 代理商ID, 允許None
        :param str logo_name: 允許None
        :param int kiosk_id: 店家ID, 允許None
        :param str kiosk_name: 允許None
        :param str custom_data: 自訂擴充定義與值,允許None
        :param float device_create_ts: 裝置綁定時間time.time(), 允許None, 若None系統會用現在時間
        """
        fun_name = 'bind_device'

        udid_err, udid_result = str_valid(fun_name, 'udid', udid, '', allow_none=False)
        if udid_err is not None:
            self.logger.error(udid_err + ' and required')
            return False

        device_create_err, device_create_ts_result = timestamp_valid(fun_name, 'device_create_ts', device_create_ts,
                                                                     allow_none=True)
        if device_create_err is not None:
            self.logger.info(device_create_err)

        if self.check_attributes:
            user_id_err, user_id_result = int_valid(fun_name, 'user_id', user_id, 0, allow_none=False)
            if user_id_err is not None:
                self.logger.warn(user_id_err)

            sys_type_err, sys_type_result = str_valid(fun_name, 'sys_type', sys_type, '', allow_none=False)
            if sys_type_err is not None:
                self.logger.warn(sys_type_err)

            sys_ver_err, sys_ver_result = str_valid(fun_name, 'sys_ver', sys_ver, '', allow_none=False)
            if sys_ver_err is not None:
                self.logger.warn(sys_ver_err)

            country_err, country_result = str_valid(fun_name, 'country', country, '', allow_none=False)
            if country_err is not None:
                self.logger.warn(country_err)

            curr_channel_err, curr_channel_result = str_valid(fun_name, 'curr_channel', curr_channel, '',
                                                              allow_none=False)
            if curr_channel_err is not None:
                self.logger.warn(curr_channel_err)

            publish_ver_err, publish_ver_result = str_valid(fun_name, 'publish_ver', publish_ver, '', allow_none=False)
            if publish_ver_err is not None:
                self.logger.warn(publish_ver_err)

            install_ts_err, install_ts_result = timestamp_valid_base(fun_name, 'install_ts', install_ts,
                                                                     allow_none=True)
            if install_ts_err is not None:
                self.logger.info(install_ts_err)

            logo_id_err, logo_id_result = int_valid(fun_name, 'logo_id', logo_id, 0, allow_none=True)
            if logo_id_err is not None:
                self.logger.info(logo_id_err)

            kiosk_id_err, kiosk_id_result = int_valid(fun_name, 'kiosk_id', kiosk_id, 0, allow_none=True)
            if kiosk_id_err is not None:
                self.logger.info(kiosk_id_err)
        else:
            user_id_result = 0 if user_id is None else user_id
            sys_type_result = str_valid_base(sys_type, '')
            sys_ver_result = str_valid_base(sys_ver, '')
            country_result = str_valid_base(country, '')
            curr_channel_result = str_valid_base(curr_channel, '')
            publish_ver_result = str_valid_base(publish_ver, '')
            install_ts_err, install_ts_result = timestamp_valid_base(fun_name, 'install_ts', install_ts,
                                                                     allow_none=True)
            logo_id_result = 0 if logo_id is None else logo_id
            kiosk_id_result = 0 if kiosk_id is None else kiosk_id
        dt = self.get_logger_datetime(device_create_ts_result)
        dc = {
            'ProDate': self.get_dt_pro_date(dt),
            'DeviceCreateTs': device_create_ts_result,
            'UserID': user_id_result,
            'Nickname': str_valid_base(nickname, '', replace_special_word=True),
            'UDID': udid_result,
            'AppsflyerID': str_valid_base(appsflyer_id, ''),
            'SysType': sys_type_result,
            'SysVer': sys_ver_result,
            'Country': country_result,
            'Region': str_valid_base(region, ''),
            'City': str_valid_base(city, ''),
            'Channel': str_valid_base(channel, ''),
            'CurrChannel': curr_channel_result,
            'InstallSource': str_valid_base(install_source, ''),
            'InstallTs': install_ts_result,
            'PublishVer': publish_ver_result,
            'LogoID': logo_id_result,
            'LogoName': str_valid_base(logo_name, ''),
            'KioskID': kiosk_id_result,
            'KioskName': str_valid_base(kiosk_name, ''),
            'RoleID': str_valid_base(role_id, ''),
            'RoleNickname': str_valid_base(role_nickname, '', replace_special_word=True),
            'DEV': str_valid_base(dev, ''),
            'MAC': str_valid_base(mac, ''),
            'AndroidID': str_valid_base(android_id, ''),
            'AAID': str_valid_base(aaid, ''),
            'IMEI': str_valid_base(imei, ''),
            'IDFA': str_valid_base(idfa, ''),
            'IDFV': str_valid_base(idfv, ''),
            'FID': str_valid_base(fid, ''),
            'CustomData': str_valid_base(custom_data, '', replace_special_word=False),
        }
        self.ark_data_manager.send("DeviceCreate", dc, device_create_ts_result, callback=self.callback,
                                   mongo_coll_date=self.get_dt_mongo_coll_date(dt))
        return True

    # 4.註冊平台帳號：AccountCreate
    def register(self, user_id, sys_type, sys_ver, country, channel, publish_ver,
                 udid=None, appsflyer_id=None, region=None, city=None, role_id=None, role_nickname=None, dev=None,
                 ip=None, longitude=None, latitude=None,
                 nickname=None, install_source=None, install_ts=None, logo_id=None, logo_name=None, kiosk_id=None,
                 kiosk_name=None, time_zone=None, lang=None, open_type=None, open_id=None,
                 open_nickname=None, custom_data=None, account_create_ts=None):
        """
        :param int user_id: 玩家ID帳號
        :param str sys_type: 操作系統
        :param str sys_ver: 操作系統版本
        :param str country: 所在地區(國別)
        :param str channel: 下載平台渠道
        :param str publish_ver: 遊戲版本
        :param str udid: 裝置ID, 允許None
        :param str appsflyer_id: 媒體下載歸因裝置ID, 允許None
        :param str region: 所在地區省州, 允許None
        :param str city: 所在地區城市, 允許None
        :param str role_id: 玩家角色ID, 允許None
        :param str role_nickname: 玩家角色暱稱, 允許None
        :param str dev: 機型, 允許None
        :param str ip: 玩家登入IP, 允許None
        :param float longitude: 經度, 允許None
        :param float latitude: 緯度, 允許None
        :param str nickname: 暱稱,允許None
        :param str install_source: 第三方媒體首次安裝來源,允許None
        :param float install_ts: 第三方媒體首次安裝時間time.time(), 允許None(0)
        :param int logo_id: 代理商ID, 允許None
        :param str logo_name: 允許None
        :param int kiosk_id: 店家ID, 允許None
        :param str kiosk_name: 允許None
        :param float time_zone: 玩家時區,允許None
        :param str lang: 使用語系,允許None
        :param str open_type: 第三方驗證者, 允許None
        :param str open_id: 第三方驗證使用者ID, 允許None
        :param str open_nickname: 第三方驗證使用者暱稱, 允許None
        :param str custom_data: 自訂擴充定義與值,允許None
        :param float account_create_ts: 帳號建立時間time.time(), 允許None, 若None系統會用現在時間
        """
        fun_name = 'register'

        user_id_err, user_id_result = int_valid(fun_name, 'user_id', user_id, 0, allow_none=False)
        if user_id_err is not None:
            self.logger.error(user_id_err + ' and required')
            return False

        account_create_err, account_create_ts_result = timestamp_valid(fun_name, 'account_create_ts', account_create_ts,
                                                                       allow_none=True)
        if account_create_err is not None:
            self.logger.info(account_create_err)

        if self.check_attributes:
            sys_type_err, sys_type_result = str_valid(fun_name, 'sys_type', sys_type, '', allow_none=False)
            if sys_type_err is not None:
                self.logger.warn(sys_type_err)

            sys_ver_err, sys_ver_result = str_valid(fun_name, 'sys_ver', sys_ver, '', allow_none=False)
            if sys_ver_err is not None:
                self.logger.warn(sys_ver_err)

            country_err, country_result = str_valid(fun_name, 'country', country, '', allow_none=False)
            if country_err is not None:
                self.logger.warn(country_err)

            channel_err, channel_result = str_valid(fun_name, 'channel', channel, '', allow_none=False)
            if channel_err is not None:
                self.logger.warn(channel_err)

            publish_ver_err, publish_ver_result = str_valid(fun_name, 'publish_ver', publish_ver, '', allow_none=False)
            if publish_ver_err is not None:
                self.logger.warn(publish_ver_err)

            longitude_err, longitude_result = longitude_valid(fun_name, 'longitude', longitude, -999, allow_none=True)
            if longitude_err is not None:
                self.logger.info(longitude_err)

            latitude_err, latitude_result = latitude_valid(fun_name, 'latitude', latitude, -999, allow_none=True)
            if latitude_err is not None:
                self.logger.info(latitude_err)

            install_ts_err, install_ts_result = timestamp_valid_base(fun_name, 'install_ts', install_ts,
                                                                     allow_none=True)
            if install_ts_err is not None:
                self.logger.info(install_ts_err)

            logo_id_err, logo_id_result = int_valid(fun_name, 'logo_id', logo_id, 0, allow_none=True)
            if logo_id_err is not None:
                self.logger.info(logo_id_err)

            kiosk_id_err, kiosk_id_result = int_valid(fun_name, 'kiosk_id', kiosk_id, 0, allow_none=True)
            if kiosk_id_err is not None:
                self.logger.info(kiosk_id_err)

            time_zone_err, time_zone_result = float_valid(fun_name, 'time_zone', time_zone, -999, allow_none=True)
            if time_zone_err is not None:
                self.logger.info(time_zone_err)
        else:
            sys_type_result = str_valid_base(sys_type, '')
            sys_ver_result = str_valid_base(sys_ver, '')
            country_result = str_valid_base(country, '')
            channel_result = str_valid_base(channel, '')
            publish_ver_result = str_valid_base(publish_ver, '')
            longitude_result = -999 if longitude is None else longitude
            latitude_result = -999 if latitude is None else latitude
            install_ts_err, install_ts_result = timestamp_valid_base(fun_name, 'install_ts', install_ts,
                                                                     allow_none=True)
            logo_id_result = 0 if logo_id is None else logo_id
            kiosk_id_result = 0 if kiosk_id is None else kiosk_id
            time_zone_result = -999 if time_zone is None else time_zone
        dt = self.get_logger_datetime(account_create_ts_result)
        ac = {
            'ProDate': self.get_dt_pro_date(dt),
            'AccountCreateTs': account_create_ts_result,
            'UserID': user_id_result,
            'Nickname': str_valid_base(nickname, '', replace_special_word=True),
            'UDID': str_valid_base(udid, ''),
            'AppsflyerID': str_valid_base(appsflyer_id, ''),
            'SysType': sys_type_result,
            'SysVer': sys_ver_result,
            'Country': country_result,
            'Region': str_valid_base(region, ''),
            'City': str_valid_base(city, ''),
            'Channel': channel_result,
            'InstallSource': str_valid_base(install_source, ''),
            'InstallTs': install_ts_result,
            'PublishVer': publish_ver_result,
            'LogoID': logo_id_result,
            'LogoName': str_valid_base(logo_name, ''),
            'KioskID': kiosk_id_result,
            'KioskName': str_valid_base(kiosk_name, ''),
            'RoleID': str_valid_base(role_id, ''),
            'RoleNickname': str_valid_base(role_nickname, '', replace_special_word=True),
            'OpenType': str_valid_base(open_type, ''),
            'OpenID': str_valid_base(open_id, ''),
            'OpenNickname': str_valid_base(open_nickname, '', replace_special_word=True),
            'DEV': str_valid_base(dev, ''),
            'IP': str_valid_base(ip, ''),
            'Longitude': longitude_result,
            'Latitude': latitude_result,
            'TimeZone': time_zone_result,
            'Lang': str_valid_base(lang, ''),
            'CustomData': str_valid_base(custom_data, '', replace_special_word=False),
        }
        self.ark_data_manager.send("AccountCreate", ac, account_create_ts_result, callback=self.callback,
                                   mongo_coll_date=self.get_dt_mongo_coll_date(dt))
        return True

    # 5.儲值成功或失敗：GameConsume 或 GameConsumeFailed, GameConsumeGetCoin, GameConsumeGetItem
    def pay(self, user_id, order_id, vender_order_id, result_code, unit_price, charge_point, sale_type, buy_number,
            buy_number_actual, buy_number_nt, currency_name, curr_channel, install_source, distributor, sys_type,
            sys_ver, country, lv, vip_lv, coin_list, item_list, coin_amount=None, vp_awarded=None, center_trans_id=None,
            center_pay_type=None, exchange_rate=None, coin_exchange_rate=None, publish_ver=None, package_type=None,
            sale_code=None, sale_name=None, scene_state=None, source_type=None, bonus_rate=None, user_memo=None,
            udid=None, appsflyer_id=None, region=None, city=None, role_id=None, role_nickname=None, dev=None,
            mac=None, android_id=None, aaid=None, imei=None, idfa=None, idfv=None, fid=None, ip=None, network=None,
            nickname=None, account_create_ts=None, channel=None, logo_id=None, logo_name=None, kiosk_id=None,
            kiosk_name=None, custom_data=None, error_source=None, error_code=None, error_message=None, pay_ts=None):
        """
        :param int user_id: 玩家ID帳號
        :param str order_id: 儲值訂單編號
        :param str vender_order_id: 廠商訂單編號
        :param int result_code: 0=儲值成功；其他=儲值失敗
        :param float unit_price: 儲值單價
        :param str charge_point: 計費點代碼
        :param str sale_type: 商品類型
        :param float buy_number: 儲值金額(統計幣別)
        :param float buy_number_actual: 原始交易金額
        :param float buy_number_nt: 訂單儲值金額(新台幣幣值)
        :param str currency_name: 原始交易幣別
        :param str curr_channel: 玩家登入渠道
        :param str install_source: 第三方媒體首次安裝來源
        :param str distributor: 儲值管道商
        :param str sys_type: 操作系統
        :param str sys_ver: 操作系統版本
        :param str country: 所在地區(國別)
        :param int lv: 目前等級
        :param int vip_lv: 目前VIP等級
        :param list coin_list: [{"coin_id":str虛擬幣代碼,"coin_amount":float,int異動數量, "bonus_coin_amount":float,int贈送數量, "end_coin_balance":float,int異動後餘額,"custom_data":str自定義}]
        :param list item_list: [{"item_id":str道具代碼, "item_amount":float,int獲得道具數量, "bonus_item_amount":float,int贈送數量, "available_sec":int道具可用秒數, "coin_value":float,int轉換金幣價值, "end_item_balance":float,int異動後餘額,"custom_data":str自定義}]
        :param float,int coin_amount: 應得總價值, 允許None
        :param int vp_awarded: 應得VP點數, 允許None
        :param str center_trans_id: 訂單儲值中心訂單編號, 允許None
        :param int center_pay_type: 訂單儲值中心付費渠道(儲值中心), 允許None
        :param float exchange_rate: 台幣參考匯率, 允許None
        :param float coin_exchange_rate: 平台幣值匯率, 允許None
        :param str publish_ver: 遊戲版本, 允許None
        :param str package_type: 促銷包選擇性, 允許None
        :param str sale_code: 促銷活動代碼, 允許None
        :param str sale_name: 促銷活動名稱, 允許None
        :param str scene_state: 儲值場景, 允許None
        :param str source_type: 曝光來源, 允許None
        :param int bonus_rate: 優惠倍數, 允許None
        :param str user_memo: 交易備註, 允許None
        :param str udid: 裝置ID, 允許None
        :param str appsflyer_id: 媒體下載歸因裝置ID, 允許None
        :param str region: 所在地區省州, 允許None
        :param str city: 所在地區城市, 允許None
        :param str role_id: 玩家角色ID, 允許None
        :param str role_nickname: 玩家角色暱稱, 允許None
        :param str dev: 機型, 允許None
        :param str mac: 裝置MAC,允許None
        :param str android_id: 允許None
        :param str aaid: 允許None
        :param str imei: 允許None
        :param str idfa: 允許None
        :param str idfv: 允許None
        :param str fid: 允許None
        :param str ip: 玩家登入IP, 允許None
        :param str network: 聯網方式,允許None
        :param str nickname: 暱稱,允許None
        :param float account_create_ts: 帳號建立時間time.time(), 允許None(0)
        :param str channel: 下載平台渠道,允許None
        :param int logo_id: 代理商ID, 允許None
        :param str logo_name: 允許None
        :param int kiosk_id: 店家ID, 允許None
        :param str kiosk_name: 允許None
        :param str custom_data: 自訂擴充定義與值,允許None
        :param str error_source: 儲值失敗時，交易錯誤來源,允許None
        :param str error_code: 儲值失敗時，交易錯誤代碼,允許None
        :param str error_message: 儲值失敗時，交易錯誤訊息,允許None
        :param float pay_ts: 儲值時間time.time(), 允許None, 若None系統會用現在時間
        """
        fun_name = 'pay'
        user_id_err, user_id_result = int_valid(fun_name, 'user_id', user_id, 0, allow_none=False)
        if user_id_err is not None:
            self.logger.error(user_id_err + ' and required')
            return False

        order_id_err, order_id_result = str_valid(fun_name, 'order_id', order_id, '', allow_none=False)
        if order_id_err is not None:
            self.logger.error(order_id_err + ' and required')
            return False

        buy_number_err, buy_number_result = float_valid(fun_name, 'buy_number', buy_number, 0, allow_none=False)
        if buy_number_err is not None:
            self.logger.error(buy_number_err + ' and required')
            return False

        buy_number_actual_err, buy_number_actual_result = float_valid(fun_name, 'buy_number_actual', buy_number_actual,
                                                                      0, allow_none=False)
        if buy_number_actual_err is not None:
            self.logger.error(buy_number_actual_err + ' and required')
            return False

        if type(coin_list) is not list:
            self.logger.warn(fun_name + ': coin_list is list')
            coin_list = list()

        if type(item_list) is not list:
            self.logger.warn(fun_name + ': item_list is list')
            item_list = list()

        pay_err, pay_ts_result = timestamp_valid(fun_name, 'pay_ts', pay_ts, allow_none=True)
        if pay_err is not None:
            self.logger.info(pay_err)

        if self.check_attributes:
            vender_order_id_err, vender_order_id_result = str_valid(fun_name, 'vender_order_id', vender_order_id, '',
                                                                    allow_none=False)
            if vender_order_id_err is not None:
                self.logger.warn(vender_order_id_err)

            result_code_err, result_code_result = int_valid(fun_name, 'result_code', result_code, -1, allow_none=False)
            if result_code_err is not None:
                self.logger.warn(result_code_err)
                return False

            unit_price_err, unit_price_result = float_valid(fun_name, 'unit_price', unit_price, 0, allow_none=False)
            if unit_price_err is not None:
                self.logger.warn(unit_price_err)

            charge_point_err, charge_point_result = str_valid(fun_name, 'charge_point', charge_point, '',
                                                              allow_none=False)
            if charge_point_err is not None:
                self.logger.warn(charge_point_err)

            sale_type_err, sale_type_result = str_valid(fun_name, 'sale_type', sale_type, '', allow_none=False)
            if sale_type_err is not None:
                self.logger.warn(sale_type_err)

            buy_number_nt_err, buy_number_nt_result = float_valid(fun_name, 'buy_number_nt', buy_number_nt, 0,
                                                                  allow_none=False)
            if buy_number_nt_err is not None:
                self.logger.warn(buy_number_nt_err)

            currency_name_err, currency_name_result = str_valid(fun_name, 'currency_name', currency_name, '',
                                                                allow_none=False)
            if currency_name_err is not None:
                self.logger.warn(currency_name_err)

            curr_channel_err, curr_channel_result = str_valid(fun_name, 'curr_channel', curr_channel, '',
                                                              allow_none=False)
            if curr_channel_err is not None:
                self.logger.warn(curr_channel_err)

            install_source_err, install_source_result = str_valid(fun_name, 'install_source', install_source, '',
                                                                  allow_none=False)
            if install_source_err is not None:
                self.logger.warn(install_source_err)

            distributor_err, distributor_result = str_valid(fun_name, 'distributor', distributor, '', allow_none=False)
            if distributor_err is not None:
                self.logger.warn(distributor_err)

            sys_type_err, sys_type_result = str_valid(fun_name, 'sys_type', sys_type, '', allow_none=False)
            if sys_type_err is not None:
                self.logger.warn(sys_type_err)

            sys_ver_err, sys_ver_result = str_valid(fun_name, 'sys_ver', sys_ver, '', allow_none=False)
            if sys_ver_err is not None:
                self.logger.warn(sys_ver_err)

            country_err, country_result = str_valid(fun_name, 'country', country, '', allow_none=False)
            if country_err is not None:
                self.logger.warn(country_err)

            lv_err, lv_result = int_valid(fun_name, 'lv', lv, 0, allow_none=False)
            if lv_err is not None:
                self.logger.warn(lv_err)

            vip_lv_err, vip_lv_result = int_valid(fun_name, 'vip_lv', vip_lv, 0, allow_none=False)
            if vip_lv_err is not None:
                self.logger.warn(vip_lv_err)

            coin_amount_err, coin_amount_result = float_valid(fun_name, 'coin_amount', coin_amount, 0, allow_none=True)
            if coin_amount_err is not None:
                self.logger.info(coin_amount_err)

            vp_awarded_err, vp_awarded_result = int_valid(fun_name, 'vp_awarded', vp_awarded, 0, allow_none=True)
            if vp_awarded_err is not None:
                self.logger.info(vp_awarded_err)

            center_pay_type_err, center_pay_type_result = int_valid(fun_name, 'center_pay_type', center_pay_type, 0,
                                                                    allow_none=True)
            if center_pay_type_err is not None:
                self.logger.info(center_pay_type_err)

            exchange_rate_err, exchange_rate_result = float_valid(fun_name, 'exchange_rate', exchange_rate, 0,
                                                                  allow_none=True)
            if exchange_rate_err is not None:
                self.logger.info(exchange_rate_err)

            coin_exchange_rate_err, coin_exchange_rate_result = float_valid(fun_name, 'coin_exchange_rate',
                                                                            coin_exchange_rate, 0, allow_none=True)
            if coin_exchange_rate_err is not None:
                self.logger.info(coin_exchange_rate_err)

            bonus_rate_err, bonus_rate_result = int_valid(fun_name, 'bonus_rate', bonus_rate, 0, allow_none=True)
            if bonus_rate_err is not None:
                self.logger.info(bonus_rate_err)

            account_create_ts_err, account_create_ts_result = timestamp_valid_base(fun_name, 'account_create_ts',
                                                                                   account_create_ts, allow_none=True)
            if account_create_ts_err is not None:
                self.logger.info(account_create_ts_err)

            logo_id_err, logo_id_result = int_valid(fun_name, 'logo_id', logo_id, 0, allow_none=True)
            if logo_id_err is not None:
                self.logger.info(logo_id_err)

            kiosk_id_err, kiosk_id_result = int_valid(fun_name, 'kiosk_id', kiosk_id, 0, allow_none=True)
            if kiosk_id_err is not None:
                self.logger.info(kiosk_id_err)
        else:
            vender_order_id_result = str_valid_base(vender_order_id, '')
            result_code_result = 0 if result_code is None else result_code
            unit_price_result = 0 if unit_price is None else unit_price
            charge_point_result = str_valid_base(charge_point, '')
            sale_type_result = str_valid_base(sale_type, '')
            buy_number_nt_result = 0 if buy_number_nt is None else buy_number_nt
            currency_name_result = str_valid_base(currency_name, '')
            curr_channel_result = str_valid_base(curr_channel, '')
            install_source_result = str_valid_base(install_source, '')
            distributor_result = str_valid_base(distributor, '')
            sys_type_result = str_valid_base(sys_type, '')
            sys_ver_result = str_valid_base(sys_ver, '')
            country_result = str_valid_base(country, '')
            lv_result = 0 if lv is None else lv
            vip_lv_result = 0 if vip_lv is None else vip_lv
            coin_amount_result = 0 if coin_amount is None else coin_amount
            vp_awarded_result = 0 if vp_awarded is None else vp_awarded
            center_pay_type_result = 0 if center_pay_type is None else center_pay_type
            exchange_rate_result = 0 if exchange_rate is None else exchange_rate
            coin_exchange_rate_result = 0 if coin_exchange_rate is None else coin_exchange_rate
            bonus_rate_result = 0 if bonus_rate is None else bonus_rate
            account_create_ts_err, account_create_ts_result = timestamp_valid_base(fun_name, 'account_create_ts',
                                                                                   account_create_ts, allow_none=True)
            logo_id_result = 0 if logo_id is None else logo_id
            kiosk_id_result = 0 if kiosk_id is None else kiosk_id
        dt = self.get_logger_datetime(pay_ts_result)
        pro_date = self.get_dt_pro_date(dt)
        gc = {
            'ProDate': pro_date,
            'PayTs': pay_ts_result,
            'UserID': user_id_result,
            'Nickname': str_valid_base(nickname, '', replace_special_word=True),
            'UDID': str_valid_base(udid, ''),
            'AppsflyerID': str_valid_base(appsflyer_id, ''),
            'OrderID': order_id_result,
            'VenderOrderID': vender_order_id_result,
            'CenterTransID': str_valid_base(center_trans_id, ''),
            'BuyNumber': float_round(buy_number_result),
            'BuyNumberActual': float_round(buy_number_actual_result),
            'BuyNumberNT': float_round(buy_number_nt_result),
            'CenterPayType': center_pay_type_result,
            'UnitPrice': float_round(unit_price_result),
            'ChargePoint': charge_point_result,
            'CoinAmount': convert_large_numbers(coin_amount_result, self.large_numbers),
            'VPAwarded': vp_awarded_result,
            'ResultCode': result_code_result,
            'SysType': sys_type_result,
            'SysVer': sys_ver_result,
            'Country': country_result,
            'Region': str_valid_base(region, ''),
            'City': str_valid_base(city, ''),
            'Channel': str_valid_base(channel, ''),
            'CurrChannel': curr_channel_result,
            'InstallSource': install_source_result,
            'AccountCreateTs': account_create_ts_result,
            'Distributor': distributor_result,
            'PublishVer': str_valid_base(publish_ver, ''),
            'LV': lv_result,
            'VipLV': vip_lv_result,
            'LogoID': logo_id_result,
            'LogoName': str_valid_base(logo_name, ''),
            'KioskID': kiosk_id_result,
            'KioskName': str_valid_base(kiosk_name, ''),
            'RoleID': str_valid_base(role_id, ''),
            'RoleNickname': str_valid_base(role_nickname, '', replace_special_word=True),
            'DEV': str_valid_base(dev, ''),
            'MAC': str_valid_base(mac, ''),
            'AndroidID': str_valid_base(android_id, ''),
            'AAID': str_valid_base(aaid, ''),
            'IMEI': str_valid_base(imei, ''),
            'IDFA': str_valid_base(idfa, ''),
            'IDFV': str_valid_base(idfv, ''),
            'FID': str_valid_base(fid, ''),
            'IP': str_valid_base(ip, ''),
            'Network': str_valid_base(network, ''),
            'SaleType': sale_type_result,
            'PackageType': str_valid_base(package_type, ''),
            'SaleCode': str_valid_base(sale_code, ''),
            'SaleName': str_valid_base(sale_name, ''),
            'SceneState': str_valid_base(scene_state, ''),
            'SourceType': str_valid_base(source_type, ''),
            'BonusRate': bonus_rate_result,
            'CurrencyName': currency_name_result,
            'ExchangeRate': float_round(exchange_rate_result),
            'CoinExchangeRate': float_round(coin_exchange_rate_result),
            'UserMemo': str_valid_base(user_memo, ''),
            'CustomData': str_valid_base(custom_data, '', replace_special_word=False),
        }
        if result_code_result == 0:
            gc_name = 'GameConsume'
        else:
            gc_name = 'GameConsumeFailed'
            gc['ErrorSource'] = str_valid_base(error_source, '')
            gc['ErrorCode'] = str_valid_base(error_code, '')
            gc['ErrorMessage'] = str_valid_base(error_message, '')

        co_list = list()
        dic_name = 'coin_list.'
        for coin in coin_list:

            coin_id_err, coin_id_result = str_valid(fun_name, dic_name + 'coin_id', coin.get('coin_id'), '',
                                                    allow_none=False)
            if coin_id_err is not None:
                self.logger.warn(coin_id_err + ' and required')
                continue

            if self.check_attributes:
                coin_amount_err, coin_amount_result = float_valid(fun_name, dic_name + 'coin_amount',
                                                                  coin.get('coin_amount'), 0, allow_none=False)
                if coin_amount_err is not None:
                    self.logger.info(coin_amount_err)

                bonus_coin_amount_err, bonus_coin_amount_result = float_valid(fun_name, dic_name + 'bonus_coin_amount',
                                                                              coin.get('bonus_coin_amount'), 0,
                                                                              allow_none=True)
                if bonus_coin_amount_err is not None:
                    self.logger.info(bonus_coin_amount_err)
            else:
                coin_amount_result = coin.get('coin_amount', 0)
                bonus_coin_amount_result = coin.get('bonus_coin_amount', 0)

            coin_custom_data = str_valid_base(coin.get('custom_data', ''), '', replace_special_word=False)

            co = {
                'ProDate': pro_date,
                'PayTs': pay_ts_result,
                'UserID': user_id_result,
                'RoleID': str_valid_base(role_id, ''),
                'OrderID': order_id_result,
                'CoinID': coin_id_result,
                'CoinAmount': convert_large_numbers(coin_amount_result, self.large_numbers),
                'BonusCoinAmount': convert_large_numbers(bonus_coin_amount_result, self.large_numbers),
                'CustomData': coin_custom_data,
            }
            co_list.append(co)

        it_list = list()
        dic_name = 'item_list.'
        for item in item_list:

            item_id_err, item_id_result = str_valid(fun_name, dic_name + 'item_id', item.get('item_id'), '',
                                                    allow_none=False)
            if item_id_err is not None:
                self.logger.warn(item_id_err + ' and required')
                continue

            if self.check_attributes:

                item_amount_err, item_amount_result = float_valid(fun_name, dic_name + 'item_amount',
                                                                  item.get('item_amount'), 0, allow_none=False)
                if item_amount_err is not None:
                    self.logger.info(item_amount_err)

                bonus_item_amount_err, bonus_item_amount_result = float_valid(fun_name, dic_name + 'bonus_item_amount',
                                                                              item.get('bonus_item_amount'), 0,
                                                                              allow_none=True)
                if bonus_item_amount_err is not None:
                    self.logger.info(bonus_item_amount_err)

                available_sec_err, available_sec_result = int_valid(fun_name, dic_name + 'available_sec',
                                                                    item.get('available_sec'), 0, allow_none=True)
                if available_sec_err is not None:
                    self.logger.info(available_sec_err)
            else:
                item_amount_result = item.get('item_amount', 0)
                bonus_item_amount_result = item.get('bonus_item_amount', 0)
                available_sec_result = item.get('available_sec', 0)

            item_custom_data = str_valid_base(item.get('custom_data', ''), '', replace_special_word=False)

            it = {
                'ProDate': pro_date,
                'PayTs': pay_ts_result,
                'UserID': user_id_result,
                'RoleID': str_valid_base(role_id, ''),
                'OrderID': order_id_result,
                'ItemID': item_id_result,
                'ItemAmount': convert_large_numbers(item_amount_result, self.large_numbers),
                'BonusItemAmount': convert_large_numbers(bonus_item_amount_result, self.large_numbers),
                'AvailableSec': available_sec_result,
                'CustomData': item_custom_data,
            }
            it_list.append(it)

        self.ark_data_manager.send(gc_name, gc, pay_ts_result, callback=self.callback,
                                   mongo_coll_date=self.get_dt_mongo_coll_date(dt))
        for i in co_list:
            self.ark_data_manager.send('GameConsumeGetCoin', i, pay_ts_result, callback=self.callback,
                                       mongo_coll_date=self.get_dt_mongo_coll_date(dt))
        for j in it_list:
            self.ark_data_manager.send('GameConsumeGetItem', j, pay_ts_result, callback=self.callback,
                                       mongo_coll_date=self.get_dt_mongo_coll_date(dt))

        return True

    # 6.儲值品項領取完成：DeliverCoin, DeliverItem, DetailCoin, DetailItem
    def take_delivery(self, user_id, order_id, pay_date, end_lv, end_vip_lv, coin_list, item_list, role_id=None,
                      nickname=None, publish_ver=None, deliver_ts=None):
        """
        :param int user_id: 玩家ID帳號
        :param str order_id: 儲值訂單編號
        :param str pay_date: 儲值日期 YYYY-mm-DD
        :param int end_lv: 領取後等級
        :param int end_vip_lv: 領取後VIP等級
        :param list coin_list: [{"coin_id":str虛擬幣代碼,"coin_amount":float,int異動數量, "bonus_coin_amount":float,int贈送數量, "end_coin_balance":float,int異動後餘額,"custom_data":str自定義}]
        :param list item_list: [{"item_id":str道具代碼, "item_amount":float,int獲得道具數量, "bonus_item_amount":float,int贈送數量, "available_sec":int道具可用秒數, "coin_value":float,int轉換金幣價值, "end_item_balance":float,int異動後餘額,"custom_data":str自定義}]
        :param str role_id: 玩家角色ID, 允許None
        :param str nickname: 暱稱,允許None
        :param str publish_ver: 遊戲版本, 允許None
        :param float deliver_ts: 配發時間time.time(), 允許None, 若None系統會用現在時間
        """
        fun_name = 'take_delivery'
        change_reason = "TakeDelivery"

        user_id_err, user_id_result = int_valid(fun_name, 'user_id', user_id, 0, allow_none=False)
        if user_id_err is not None:
            self.logger.error(user_id_err + ' and required')
            return False

        order_id_err, order_id_result = str_valid(fun_name, 'order_id', order_id, '', allow_none=False)
        if order_id_err is not None:
            self.logger.error(order_id_err + ' and required')
            return False

        pay_date_err, pay_date_result = date_valid(fun_name, 'pay_date', pay_date, '1970-01-01', allow_none=False)
        if pay_date_err is not None:
            self.logger.error(pay_date_err + ' and required')
            return False

        deliver_err, deliver_ts_result = timestamp_valid(fun_name, 'deliver_ts', deliver_ts, allow_none=True)
        if deliver_err is not None:
            self.logger.info(deliver_err)

        if type(coin_list) is not list:
            self.logger.warn(fun_name + ': coin_list is list')
            coin_list = list()

        if type(item_list) is not list:
            self.logger.warn(fun_name + ': item_list is list')
            item_list = list()

        if self.check_attributes:
            end_lv_err, end_lv_result = int_valid(fun_name, 'end_lv', end_lv, 0, allow_none=False)
            if end_lv_err is not None:
                self.logger.warn(end_lv_err)

            end_vip_lv_err, end_vip_lv_result = int_valid(fun_name, 'end_vip_lv', end_vip_lv, 0, allow_none=False)
            if end_vip_lv_err is not None:
                self.logger.warn(end_vip_lv_err)
        else:
            end_lv_result = 0 if end_lv is None else end_lv
            end_vip_lv_result = 0 if end_vip_lv is None else end_vip_lv

        role_id_result = str_valid_base(role_id, '')
        nickname_result = str_valid_base(nickname, '', replace_special_word=True)
        publish_ver_result = str_valid_base(publish_ver, '')

        deliver_coin = list()
        detail_coin = list()
        dic_name = 'coin_list.'
        dt = self.get_logger_datetime(deliver_ts_result)
        pro_date = self.get_dt_pro_date(dt)
        for coin in coin_list:

            coin_id_err, coin_id_result = str_valid(fun_name, dic_name + 'coin_id', coin.get('coin_id'),
                                                    '', allow_none=False)
            if coin_id_err is not None:
                self.logger.warn(coin_id_err + ' and required')
                continue

            if self.check_attributes:
                coin_amount_err, coin_amount_result = float_valid(fun_name, dic_name + 'coin_amount',
                                                                  coin.get('coin_amount'), 0, allow_none=False)
                if coin_amount_err is not None:
                    self.logger.info(coin_amount_err)

                bonus_coin_amount_err, bonus_coin_amount_result = float_valid(fun_name, dic_name + 'bonus_coin_amount',
                                                                              coin.get('bonus_coin_amount'), 0,
                                                                              allow_none=True)
                if bonus_coin_amount_err is not None:
                    self.logger.info(bonus_coin_amount_err)

                end_coin_balance_err, end_coin_balance_result = float_valid(fun_name, dic_name + 'end_coin_balance',
                                                                            coin.get('end_coin_balance'), 0,
                                                                            allow_none=False)
                if end_coin_balance_err is not None:
                    self.logger.info(end_coin_balance_err)
            else:
                coin_amount_result = coin.get('coin_amount', 0)
                bonus_coin_amount_result = coin.get('bonus_coin_amount', 0)
                end_coin_balance_result = coin.get('end_coin_balance', 0)

            coin_custom_data = str_valid_base(coin.get('custom_data', ''), '', replace_special_word=False)
            coin_balance_err, coin_balance_result = float_valid(fun_name, dic_name + 'coin_balance',
                                                                coin.get('coin_balance'), None, allow_none=True)
            if coin_balance_err is not None:
                self.logger.info(coin_balance_err)
            if coin_balance_result is None:
                val = end_coin_balance_result - bonus_coin_amount_result - coin_amount_result
                coin_balance_result = convert_large_numbers(val, self.large_numbers)

            c1 = {
                'ProDate': pro_date,
                'PayDate': pay_date_result,
                'DeliveryTs': deliver_ts_result,
                'UserID': user_id_result,
                'Nickname': nickname_result,
                'RoleID': role_id_result,
                'OrderID': order_id_result,
                'CoinID': coin_id_result,
                'CoinAmount': convert_large_numbers(coin_amount_result, self.large_numbers),
                'BonusCoinAmount': convert_large_numbers(bonus_coin_amount_result, self.large_numbers),
                'CoinBalanceBefore': coin_balance_result,
                'CoinBalanceAfter': convert_large_numbers(end_coin_balance_result, self.large_numbers),
                'LVAfter': end_lv_result,
                'VipLVAfter': end_vip_lv_result,
                'PublishVer': publish_ver_result,
                'CustomData': coin_custom_data,
            }
            deliver_coin.append(c1)

            c2 = {
                'ProDate': pro_date,
                'ChangeTs': deliver_ts_result,
                'UserID': user_id_result,
                'Nickname': nickname_result,
                'CoinID': coin_id_result,
                'IncreaseCoins': 0 if positive_valid(coin_amount_result) is False else convert_large_numbers(
                    coin_amount_result, self.large_numbers),
                'DecreaseCoins': 0 if negative_valid(coin_amount_result) is False else convert_large_numbers(
                    coin_amount_result, self.large_numbers),
                'BonusCoins': convert_large_numbers(bonus_coin_amount_result, self.large_numbers),
                'CoinBalanceAfter': convert_large_numbers(end_coin_balance_result, self.large_numbers),
                'ChangeReason': change_reason,
                'SceneState': '',
                'GameID': '',
                'RoomID': '',
                'MachineID': '',
                'SeatNum': 0,
                'CoinRatio': 0,
                'PublishVer': publish_ver_result,
                'CustomData': coin_custom_data,
            }
            detail_coin.append(c2)

        deliver_item = list()
        detail_item = list()
        dic_name = 'item_list.'
        for item in item_list:

            item_id_err, item_id_result = str_valid(fun_name, dic_name + 'item_id', item.get('item_id'), '',
                                                    allow_none=False)
            if item_id_err is not None:
                self.logger.warn(item_id_err + ' and required')
                continue

            if self.check_attributes:
                item_amount_err, item_amount_result = float_valid(fun_name, dic_name + 'item_amount',
                                                                  item.get('item_amount'), 0, allow_none=False)
                if item_amount_err is not None:
                    self.logger.info(item_amount_err)

                bonus_item_amount_err, bonus_item_amount_result = float_valid(fun_name, dic_name + 'bonus_item_amount',
                                                                              item.get('bonus_item_amount'), 0,
                                                                              allow_none=True)
                if bonus_item_amount_err is not None:
                    self.logger.info(bonus_item_amount_err)

                coin_value_err, coin_value_result = float_valid(fun_name, dic_name + 'coin_value',
                                                                item.get('coin_value'), 0, allow_none=False)
                if coin_value_err is not None:
                    self.logger.info(coin_value_err)

                end_item_balance_err, end_item_balance_result = float_valid(fun_name, dic_name + 'end_item_balance',
                                                                            item.get('end_item_balance'), 0,
                                                                            allow_none=False)
                if end_item_balance_err is not None:
                    self.logger.info(end_item_balance_err)

                available_sec_err, available_sec_result = int_valid(fun_name, dic_name + 'available_sec',
                                                                    item.get('available_sec'), 0, allow_none=True)
                if available_sec_err is not None:
                    self.logger.info(available_sec_err)
            else:
                item_amount_result = item.get('item_amount', 0)
                bonus_item_amount_result = item.get('bonus_item_amount', 0)
                coin_value_result = item.get('coin_value', 0)
                end_item_balance_result = item.get('end_item_balance', 0)
                available_sec_result = item.get('available_sec', 0)

            item_custom_data = str_valid_base(item.get('custom_data', ''), '', replace_special_word=False)
            item_balance_err, item_balance_result = float_valid(fun_name, dic_name + 'item_balance',
                                                                item.get('item_balance'), None, allow_none=True)
            if item_balance_err is not None:
                self.logger.info(item_balance_err)
            if item_balance_result is None:
                val = end_item_balance_result - bonus_item_amount_result - item_amount_result
                item_balance_result = convert_large_numbers(val, self.large_numbers)

            i1 = {
                'ProDate': pro_date,
                'PayDate': pay_date_result,
                'DeliveryTs': deliver_ts_result,
                'UserID': user_id_result,
                'Nickname': nickname_result,
                'RoleID': role_id_result,
                'OrderID': order_id_result,
                'ItemID': item_id_result,
                'ItemAmount': convert_large_numbers(item_amount_result, self.large_numbers),
                'BonusItemAmount': convert_large_numbers(bonus_item_amount_result, self.large_numbers),
                'ItemBalanceBefore': item_balance_result,
                'ItemBalanceAfter': convert_large_numbers(end_item_balance_result, self.large_numbers),
                'LVAfter': end_lv_result,
                'VipLVAfter': end_vip_lv_result,
                'AvailableSec': available_sec_result,
                'CoinValue': convert_large_numbers(coin_value_result, self.large_numbers),
                'PublishVer': publish_ver_result,
                'CustomData': item_custom_data,
            }
            deliver_item.append(i1)

            i2 = {
                'ProDate': pro_date,
                'ChangeTs': deliver_ts_result,
                'UserID': user_id_result,
                'Nickname': nickname_result,
                'ItemID': item_id_result,
                'AvailableSec': available_sec_result,
                'CoinValue': convert_large_numbers(coin_value_result, self.large_numbers),
                'ChangeAmount': convert_large_numbers(item_amount_result, self.large_numbers),
                'BonusAmount': convert_large_numbers(bonus_item_amount_result, self.large_numbers),
                'ItemBalanceAfter': convert_large_numbers(end_item_balance_result, self.large_numbers),
                'ChangeReason': change_reason,
                'CoinRatio': 0,
                'SceneState': '',
                'GameID': '',
                'RoomID': '',
                'MachineID': '',
                'SeatNum': 0,
                'PublishVer': publish_ver_result,
                'CustomData': item_custom_data,
            }
            detail_item.append(i2)

        for i in deliver_coin:
            self.ark_data_manager.send('DeliverCoin', i, deliver_ts_result, callback=self.callback,
                                       mongo_coll_date=self.get_dt_mongo_coll_date(dt))
        for j in detail_coin:
            self.ark_data_manager.send('DetailCoin', j, deliver_ts_result, callback=self.callback,
                                       mongo_coll_date=self.get_dt_mongo_coll_date(dt))

        for m in deliver_item:
            self.ark_data_manager.send('DeliverItem', m, deliver_ts_result, callback=self.callback,
                                       mongo_coll_date=self.get_dt_mongo_coll_date(dt))
        for n in detail_item:
            self.ark_data_manager.send('DetailItem', n, deliver_ts_result, callback=self.callback,
                                       mongo_coll_date=self.get_dt_mongo_coll_date(dt))

        return True

    # 7.遊戲資產異動：DetailCoin, DetailItem, AssignAwardCoin, AssignAwardItem
    def sys_change_asset(self, user_id, change_reason, coin_list, item_list, country=None, curr_channel=None,
                         publish_ver=None, vp_awarded=None, vp_balance=None, lv=None, vip_lv=None, end_vip_lv=None,
                         nickname=None, event_id=None, event_name=None, logo_id=None, logo_name=None, kiosk_id=None,
                         kiosk_name=None, role_id=None, role_nickname=None, coin_ratio=None, scene_state=None,
                         game_id=None, room_id=None, machine_id=None, seat_num=None, change_ts=None):
        """
        :param int user_id: 玩家ID帳號
        :param str change_reason: 經濟系統異動原因， change_reason=AssignAward=置獎活動
        :param list coin_list: [{"coin_id":str虛擬幣代碼,"coin_amount":float,int異動數量, "bonus_coin_amount":float,int贈送數量, "end_coin_balance":float,int異動後餘額,"custom_data":str自定義}]
        :param list item_list: [{"item_id":str道具代碼, "item_amount":float,int獲得道具數量, "bonus_item_amount":float,int贈送數量, "available_sec":int道具可用秒數, "coin_value":float,int轉換金幣價值, "end_item_balance":float,int異動後餘額,"custom_data":str自定義}]
        :param str country: 所在地區(國別), 允許None
        :param str curr_channel: 玩家登入渠道, 允許None
        :param str publish_ver: 遊戲版本, 允許None
        :param int vp_awarded: 應得VP點數, 允許None
        :param int vp_balance: 置獎後VP點數, 允許None
        :param int lv: 目前等級, 允許None
        :param int vip_lv: 置獎前VIP, 允許None
        :param int end_vip_lv: 置獎後VIP, 允許None
        :param str nickname: 暱稱,允許None
        :param str event_id: 事件ID, 置獎設定代號, 允許None。 若change_reason=AssignAward，則event_id=置獎活動ID，不能為None
        :param str event_name: 事件名稱, 置獎設定名稱, 允許None
        :param int logo_id: 代理商ID, 允許None
        :param str logo_name: 允許None
        :param int kiosk_id: 店家ID, 允許None
        :param str kiosk_name: 允許None
        :param str role_id: 玩家角色ID, 允許None
        :param str role_nickname: 玩家角色暱稱, 允許None
        :param float,int coin_ratio: 機台換算幣值, 允許None
        :param str scene_state: 儲值場景, 允許None
        :param str game_id: 遊戲ID, 允許None
        :param str room_id: 廳館ID, 允許None
        :param str machine_id: 機台ID, 允許None
        :param int seat_num: 座位編號, 允許None
        :param float change_ts: 異動時間time.time(), 允許None, 若None系統會用現在時間
        """
        fun_name = 'sys_change_asset'

        user_id_err, user_id_result = int_valid(fun_name, 'user_id', user_id, 0, allow_none=False)
        if user_id_err is not None:
            self.logger.error(user_id_err + ' and required')
            return False

        change_reason_err, change_reason_result = str_valid(fun_name, 'change_reason', change_reason, 'Unknown',
                                                            allow_none=False)
        if change_reason_err is not None:
            self.logger.warn(change_reason_err + ' and required')
        if len(change_reason_result) == 0:
            self.logger.warn('%s: change_reason is %s it must be str and required' % (fun_name, change_reason))
            change_reason_result = 'Unknown'

        if type(coin_list) is not list:
            self.logger.warn(fun_name + ': coin_list is list')
            coin_list = list()

        if type(item_list) is not list:
            self.logger.warn(fun_name + ': item_list is list')
            item_list = list()

        change_err, change_ts_result = timestamp_valid(fun_name, 'deliver_ts', change_ts, allow_none=True)
        if change_err is not None:
            self.logger.info(change_err)

        if self.check_attributes:
            vp_awarded_err, vp_awarded_result = int_valid(fun_name, 'vp_awarded', vp_awarded, 0, allow_none=True)
            if vp_awarded_err is not None:
                self.logger.info(vp_awarded_err)

            vp_balance_err, vp_balance_result = int_valid(fun_name, 'vp_balance', vp_balance, 0, allow_none=True)
            if vp_balance_err is not None:
                self.logger.info(vp_balance_err)

            lv_err, lv_result = int_valid(fun_name, 'lv', lv, 0, allow_none=True)
            if lv_err is not None:
                self.logger.info(lv_err)

            vip_lv_err, vip_lv_result = int_valid(fun_name, 'vip_lv', vip_lv, 0, allow_none=True)
            if vip_lv_err is not None:
                self.logger.info(vip_lv_err)

            end_vip_lv_err, end_vip_lv_result = int_valid(fun_name, 'end_vip_lv', end_vip_lv, 0, allow_none=True)
            if end_vip_lv_err is not None:
                self.logger.info(end_vip_lv_err)

            logo_id_err, logo_id_result = int_valid(fun_name, 'logo_id', logo_id, 0, allow_none=True)
            if logo_id_err is not None:
                self.logger.info(logo_id_err)

            kiosk_id_err, kiosk_id_result = int_valid(fun_name, 'kiosk_id', kiosk_id, 0, allow_none=True)
            if kiosk_id_err is not None:
                self.logger.info(kiosk_id_err)

            coin_ratio_err, coin_ratio_result = float_valid(fun_name, 'coin_ratio', coin_ratio, 0, allow_none=True)
            if coin_ratio_err is not None:
                self.logger.info(coin_ratio_err)

            seat_num_err, seat_num_result = int_valid(fun_name, 'seat_num', seat_num, 0, allow_none=True)
            if seat_num_err is not None:
                self.logger.info(seat_num_err)

        else:
            vp_awarded_result = 0 if vp_awarded is None else vp_awarded
            vp_balance_result = 0 if vp_balance is None else vp_balance
            lv_result = 0 if lv is None else lv
            vip_lv_result = 0 if vip_lv is None else vip_lv
            end_vip_lv_result = 0 if end_vip_lv is None else end_vip_lv
            logo_id_result = 0 if logo_id is None else logo_id
            kiosk_id_result = 0 if kiosk_id is None else kiosk_id
            coin_ratio_result = 0 if coin_ratio is None else coin_ratio
            seat_num_result = 0 if seat_num is None else seat_num

        if change_reason_result == 'AssignAward' and str_valid_base(event_id, '') == '':
            self.logger.warn(fun_name + ': change_reason is AssignAward event_id is not empty')

        country_result = str_valid_base(country, '')
        curr_channel_result = str_valid_base(curr_channel, '')
        publish_ver_result = str_valid_base(publish_ver, '')

        dt = self.get_logger_datetime(change_ts_result)
        pro_date = self.get_dt_pro_date(dt)
        detail_coin = list()
        assign_award_coin = list()
        dic_name = 'coin_list.'
        for coin in coin_list:

            coin_id_err, coin_id_result = str_valid(fun_name, dic_name + 'coin_id', coin.get('coin_id'), '',
                                                    allow_none=False)
            if coin_id_err is not None:
                self.logger.warn(coin_id_err + ' and required')
                continue

            if self.check_attributes:
                coin_amount_err, coin_amount_result = float_valid(fun_name, dic_name + 'coin_amount',
                                                                  coin.get('coin_amount'), 0, allow_none=False)
                if coin_amount_err is not None:
                    self.logger.info(coin_amount_err)

                bonus_coin_amount_err, bonus_coin_amount_result = float_valid(fun_name, dic_name + 'bonus_coin_amount',
                                                                              coin.get('bonus_coin_amount'), 0,
                                                                              allow_none=True)
                if bonus_coin_amount_err is not None:
                    self.logger.info(bonus_coin_amount_err)

                end_coin_balance_err, end_coin_balance_result = float_valid(fun_name, dic_name + 'end_coin_balance',
                                                                            coin.get('end_coin_balance'), 0,
                                                                            allow_none=False)
                if end_coin_balance_err is not None:
                    self.logger.info(end_coin_balance_err)
            else:
                coin_amount_result = coin.get('coin_amount', 0)
                bonus_coin_amount_result = coin.get('bonus_coin_amount', 0)
                end_coin_balance_result = coin.get('end_coin_balance', 0)

            coin_custom_data = str_valid_base(coin.get('custom_data', ''), '', replace_special_word=False)
            coin_balance_err, coin_balance_result = float_valid(fun_name, dic_name + 'coin_balance',
                                                                coin.get('coin_balance'), None, allow_none=True)
            if coin_balance_err is not None:
                self.logger.info(coin_balance_err)
            if coin_balance_result is None:
                val = end_coin_balance_result - bonus_coin_amount_result - coin_amount_result
                coin_balance_result = convert_large_numbers(val, self.large_numbers)

            c1 = {
                'ProDate': pro_date,
                'ChangeTs': change_ts_result,
                'UserID': user_id_result,
                'Nickname': str_valid_base(nickname, '', replace_special_word=True),
                'CoinID': coin_id_result,
                'IncreaseCoins': 0 if positive_valid(coin_amount_result) is False else convert_large_numbers(
                    coin_amount_result, self.large_numbers),
                'DecreaseCoins': 0 if negative_valid(coin_amount_result) is False else convert_large_numbers(
                    coin_amount_result, self.large_numbers),
                'BonusCoins': convert_large_numbers(bonus_coin_amount_result, self.large_numbers),
                'CoinBalanceAfter': convert_large_numbers(end_coin_balance_result, self.large_numbers),
                'ChangeReason': change_reason_result,
                'SceneState': str_valid_base(scene_state, ''),
                'GameID': str_valid_base(game_id, ''),
                'RoomID': str_valid_base(room_id, ''),
                'MachineID': str_valid_base(machine_id, ''),
                'SeatNum': seat_num_result,
                'CoinRatio': convert_large_numbers(coin_ratio_result, self.large_numbers),
                'PublishVer': publish_ver_result,
                'CustomData': coin_custom_data,
            }
            detail_coin.append(c1)

            if change_reason == 'AssignAward':
                c2 = {
                    'ProDate': pro_date,
                    'RewardTs': change_ts_result,
                    'UserID': user_id_result,
                    'Nickname': str_valid_base(nickname, '', replace_special_word=True),
                    'AssignID': str_valid_base(event_id, ''),
                    'AssignName': str_valid_base(event_name, ''),
                    'CoinID': coin_id_result,
                    'CoinAmount': convert_large_numbers(coin_amount_result + bonus_coin_amount_result,
                                                        self.large_numbers),
                    'CoinBalanceBefore': coin_balance_result,
                    'CoinBalanceAfter': convert_large_numbers(end_coin_balance_result, self.large_numbers),
                    'Country': country_result,
                    'CurrChannel': curr_channel_result,
                    'PublishVer': publish_ver_result,
                    'LV': lv_result,
                    'VipLVBefore': vip_lv_result,
                    'VipLVAfter': end_vip_lv_result,
                    'VPAwarded': vp_awarded_result,
                    'VPAfter': vp_balance_result,
                    'LogoID': logo_id_result,
                    'LogoName': str_valid_base(logo_name, ''),
                    'KioskID': kiosk_id_result,
                    'KioskName': str_valid_base(kiosk_name, ''),
                    'RoleID': str_valid_base(role_id, ''),
                    'RoleNickname': str_valid_base(role_nickname, '', replace_special_word=True),
                    'CustomData': coin_custom_data,
                }
                assign_award_coin.append(c2)

        detail_item = list()
        assign_award_item = list()
        dic_name = 'item_list.'
        for item in item_list:

            item_id_err, item_id_result = str_valid(fun_name, dic_name + 'item_id', item.get('item_id'), '',
                                                    allow_none=False)
            if item_id_err is not None:
                self.logger.warn(item_id_err + ' and required')
                continue

            if self.check_attributes:
                item_amount_err, item_amount_result = float_valid(fun_name, dic_name + 'item_amount',
                                                                  item.get('item_amount'), 0, allow_none=False)
                if item_amount_err is not None:
                    self.logger.info(item_amount_err)

                bonus_item_amount_err, bonus_item_amount_result = float_valid(fun_name, dic_name + 'bonus_item_amount',
                                                                              item.get('bonus_item_amount'), 0,
                                                                              allow_none=True)
                if bonus_item_amount_err is not None:
                    self.logger.info(bonus_item_amount_err)

                coin_value_err, coin_value_result = float_valid(fun_name, dic_name + 'coin_value',
                                                                item.get('coin_value'), 0, allow_none=False)
                if coin_value_err is not None:
                    self.logger.info(coin_value_err)

                end_item_balance_err, end_item_balance_result = float_valid(fun_name, dic_name + 'end_item_balance',
                                                                            item.get('end_item_balance'), 0,
                                                                            allow_none=False)
                if end_item_balance_err is not None:
                    self.logger.info(end_item_balance_err)

                available_sec_err, available_sec_result = int_valid(fun_name, dic_name + 'available_sec',
                                                                    item.get('available_sec'), 0, allow_none=True)
                if available_sec_err is not None:
                    self.logger.info(available_sec_err)
            else:
                item_amount_result = item.get('item_amount', 0)
                bonus_item_amount_result = item.get('bonus_item_amount', 0)
                coin_value_result = item.get('coin_value', 0)
                end_item_balance_result = item.get('end_item_balance', 0)
                available_sec_result = item.get('available_sec', 0)

            item_custom_data = str_valid_base(item.get('custom_data', ''), '', replace_special_word=False)
            item_balance_err, item_balance_result = float_valid(fun_name, dic_name + 'item_balance',
                                                                item.get('item_balance'), None, allow_none=True)
            if item_balance_err is not None:
                self.logger.info(item_balance_err)
            if item_balance_result is None:
                val = end_item_balance_result - bonus_item_amount_result - item_amount_result
                item_balance_result = convert_large_numbers(val, self.large_numbers)

            i1 = {
                'ProDate': pro_date,
                'ChangeTs': change_ts_result,
                'UserID': user_id_result,
                'Nickname': str_valid_base(nickname, '', replace_special_word=True),
                'ItemID': item_id_result,
                'AvailableSec': available_sec_result,
                'CoinValue': convert_large_numbers(coin_value_result, self.large_numbers),
                'ChangeAmount': convert_large_numbers(item_amount_result, self.large_numbers),
                'BonusAmount': convert_large_numbers(bonus_item_amount_result, self.large_numbers),
                'ItemBalanceAfter': convert_large_numbers(end_item_balance_result, self.large_numbers),
                'ChangeReason': change_reason,
                'CoinRatio': convert_large_numbers(coin_ratio_result, self.large_numbers),
                'SceneState': str_valid_base(scene_state, ''),
                'GameID': str_valid_base(game_id, ''),
                'RoomID': str_valid_base(room_id, ''),
                'MachineID': str_valid_base(machine_id, ''),
                'SeatNum': seat_num_result,
                'PublishVer': publish_ver_result,
                'CustomData': item_custom_data,
            }
            detail_item.append(i1)

            if change_reason == 'AssignAward':
                i2 = {
                    'ProDate': pro_date,
                    'RewardTs': change_ts_result,
                    'UserID': user_id_result,
                    'Nickname': str_valid_base(nickname, '', replace_special_word=True),
                    'AssignID': str_valid_base(event_id, ''),
                    'AssignName': str_valid_base(event_name, ''),
                    'ItemID': item_id_result,
                    'ItemAmount': convert_large_numbers(item_amount_result + bonus_item_amount_result,
                                                        self.large_numbers),
                    'ItemBalanceBefore': item_balance_result,
                    'ItemBalanceAfter': convert_large_numbers(end_item_balance_result, self.large_numbers),
                    'Country': country_result,
                    'CurrChannel': curr_channel_result,
                    'PublishVer': publish_ver_result,
                    'LV': lv_result,
                    'VipLVBefore': vip_lv_result,
                    'VipLVAfter': end_vip_lv_result,
                    'CoinValue': convert_large_numbers(coin_value_result, self.large_numbers),
                    'LogoID': logo_id_result,
                    'LogoName': str_valid_base(logo_name, ''),
                    'KioskID': kiosk_id_result,
                    'KioskName': str_valid_base(kiosk_name, ''),
                    'RoleID': str_valid_base(role_id, ''),
                    'RoleNickname': str_valid_base(role_nickname, '', replace_special_word=True),
                    'CustomData': item_custom_data,
                }
                assign_award_item.append(i2)

        for i in detail_coin:
            self.ark_data_manager.send('DetailCoin', i, change_ts_result, callback=self.callback,
                                       mongo_coll_date=self.get_dt_mongo_coll_date(dt))
        for j in assign_award_coin:
            self.ark_data_manager.send('AssignAwardCoin', j, change_ts_result, callback=self.callback,
                                       mongo_coll_date=self.get_dt_mongo_coll_date(dt))

        for m in detail_item:
            self.ark_data_manager.send('DetailItem', m, change_ts_result, callback=self.callback,
                                       mongo_coll_date=self.get_dt_mongo_coll_date(dt))
        for n in assign_award_item:
            self.ark_data_manager.send('AssignAwardItem', n, change_ts_result, callback=self.callback,
                                       mongo_coll_date=self.get_dt_mongo_coll_date(dt))

        return True

    # 8.遊戲資產交易或贈禮：DetailCoin, DetailItem, TransferCoin, TransferItem
    def player_transfer(self, transfer_id, sender_user_id, sender_vip_lv, receiver_user_id, sender_coin_list,
                        sender_item_list, receiver_coin_list, receiver_item_list, sender_nickname=None,
                        receiver_nickname=None, sender_publish_ver=None, receiver_publish_ver=None, transfer_ts=None):
        """
        :param str transfer_id: 收贈ID
        :param int sender_user_id: 贈禮者user_id
        :param int sender_vip_lv: 贈禮者VIP等級
        :param int receiver_user_id: 收禮者user_id
        :param list sender_coin_list: 贈禮者coin清單 [{"coin_id":str虛擬幣代碼,"coin_amount":float,int異動數量, "bonus_coin_amount":float,int贈送數量, "end_coin_balance":float,int異動後餘額,"custom_data":str自定義}]
        :param list sender_item_list: 贈禮者道具清單 [{"item_id":str道具代碼, "item_amount":float,int獲得道具數量, "bonus_item_amount":float,int贈送數量, "available_sec":int道具可用秒數, "coin_value":float,int轉換金幣價值, "end_item_balance":float,int異動後餘額,"custom_data":str自定義}]
        :param list receiver_coin_list: 收禮者coin清單 [{"coin_id":str虛擬幣代碼,"coin_amount":float,int異動數量, "bonus_coin_amount":float,int贈送數量, "end_coin_balance":float,int異動後餘額,"custom_data":str自定義}]
        :param list receiver_item_list: 收禮者道具清單 [{"item_id":str道具代碼, "item_amount":float,int獲得道具數量, "bonus_item_amount":float,int贈送數量, "available_sec":int道具可用秒數, "coin_value":float,int轉換金幣價值, "end_item_balance":float,int異動後餘額,"custom_data":str自定義}]
        :param str sender_nickname: 贈禮者暱稱, 允許None
        :param str receiver_nickname: 收禮者暱稱, 允許None
        :param str sender_publish_ver: 贈禮者遊戲版本, 允許None
        :param str receiver_publish_ver:  收禮者遊戲版本, 允許None
        :param float transfer_ts: 收贈時間time.time(), 允許None, 若None系統會用現在時間
        """
        fun_name = 'player_transfer'
        change_reason = "PlayerTransfer"

        transfer_id_err, transfer_id_result = str_valid(fun_name, 'transfer_id', transfer_id, '', allow_none=False)
        if transfer_id_err is not None:
            self.logger.error(transfer_id_err + ' and required')
            return False

        sender_user_id_err, sender_user_id_result = int_valid(fun_name, 'sender_user_id', sender_user_id, 0,
                                                              allow_none=False)
        if sender_user_id_err is not None:
            self.logger.error(sender_user_id_err + ' and required')
            return False

        receiver_user_id_err, receiver_user_id_result = int_valid(fun_name, 'receiver_user_id', receiver_user_id, 0,
                                                                  allow_none=False)
        if receiver_user_id_err is not None:
            self.logger.error(receiver_user_id_err + ' and required')
            return False

        transfer_err, transfer_ts_result = timestamp_valid(fun_name, 'transfer_ts', transfer_ts, allow_none=True)
        if transfer_err is not None:
            self.logger.info(transfer_err)

        if type(sender_coin_list) is not list:
            self.logger.warn(fun_name + ': sender_coin_list is list')
            sender_coin_list = list()

        if type(sender_item_list) is not list:
            self.logger.warn(fun_name + ': sender_item_list is list')
            sender_item_list = list()

        if type(receiver_coin_list) is not list:
            self.logger.warn(fun_name + ': receiver_coin_list is list')
            receiver_coin_list = list()

        if type(receiver_item_list) is not list:
            self.logger.warn(fun_name + ': receiver_item_list is list')
            receiver_item_list = list()

        if self.check_attributes:

            sender_vip_lv_err, sender_vip_lv_result = int_valid(fun_name, 'sender_vip_lv', sender_vip_lv, 0,
                                                                allow_none=False)
            if sender_vip_lv_err is not None:
                self.logger.warn(sender_vip_lv_err)

            sender_nickname_err, sender_nickname_result = str_valid(fun_name, 'sender_nickname', sender_nickname, '',
                                                                    allow_none=True, replace_special_word=True)
            if sender_nickname_err is not None:
                self.logger.info(sender_nickname_err)

            receiver_nickname_err, receiver_nickname_result = str_valid(fun_name, 'receiver_nickname',
                                                                        receiver_nickname, '', allow_none=True, replace_special_word=True)
            if receiver_nickname_err is not None:
                self.logger.info(receiver_nickname_err)

        else:
            sender_vip_lv_result = 0 if sender_vip_lv is None else sender_vip_lv
            sender_nickname_result = '' if sender_nickname is None else sender_nickname
            receiver_nickname_result = '' if receiver_nickname is None else receiver_nickname

        sender_publish_ver_result = str_valid_base(sender_publish_ver, '')
        receiver_publish_ver_result = str_valid_base(receiver_publish_ver, '')

        dt = self.get_logger_datetime(transfer_ts_result)
        pro_date = self.get_dt_pro_date(dt)
        detail_coin = list()
        transfer_coin = list()
        dic_name = 'sender_coin_list.'
        for coin in sender_coin_list:

            coin_id_err, coin_id_result = str_valid(fun_name, dic_name + 'coin_id', coin.get('coin_id'), '',
                                                    allow_none=False)
            if coin_id_err is not None:
                self.logger.warn(coin_id_err + ' and required')
                continue

            if self.check_attributes:
                coin_amount_err, coin_amount_result = float_valid(fun_name, dic_name + 'coin_amount',
                                                                  coin.get('coin_amount'), 0, allow_none=False)
                if coin_amount_err is not None:
                    self.logger.info(coin_amount_err)

                bonus_coin_amount_err, bonus_coin_amount_result = float_valid(fun_name, dic_name + 'bonus_coin_amount',
                                                                              coin.get('bonus_coin_amount'), 0,
                                                                              allow_none=True)
                if bonus_coin_amount_err is not None:
                    self.logger.info(bonus_coin_amount_err)

                end_coin_balance_err, end_coin_balance_result = float_valid(fun_name, dic_name + 'end_coin_balance',
                                                                            coin.get('end_coin_balance'), 0,
                                                                            allow_none=False)
                if end_coin_balance_err is not None:
                    self.logger.info(end_coin_balance_err)
            else:
                coin_amount_result = coin.get('coin_amount', 0)
                bonus_coin_amount_result = coin.get('bonus_coin_amount', 0)
                end_coin_balance_result = coin.get('end_coin_balance', 0)

            coin_custom_data = str_valid_base(coin.get('custom_data', ''), '', replace_special_word=False)
            coin_balance_err, coin_balance_result = float_valid(fun_name, dic_name + 'coin_balance',
                                                                coin.get('coin_balance'), None, allow_none=True)
            if coin_balance_err is not None:
                self.logger.info(coin_balance_err)
            if coin_balance_result is None:
                val = end_coin_balance_result - bonus_coin_amount_result - coin_amount_result
                coin_balance_result = convert_large_numbers(val, self.large_numbers)

            c1 = {
                'ProDate': pro_date,
                'ChangeTs': transfer_ts_result,
                'UserID': sender_user_id_result,
                'Nickname': sender_nickname_result,
                'CoinID': coin_id_result,
                'IncreaseCoins': 0 if positive_valid(coin_amount_result) is False else convert_large_numbers(
                    coin_amount_result, self.large_numbers),
                'DecreaseCoins': 0 if negative_valid(coin_amount_result) is False else convert_large_numbers(
                    coin_amount_result, self.large_numbers),
                'BonusCoins': convert_large_numbers(bonus_coin_amount_result, self.large_numbers),
                'CoinBalanceAfter': convert_large_numbers(end_coin_balance_result, self.large_numbers),
                'ChangeReason': change_reason,
                'SceneState': '',
                'GameID': '',
                'RoomID': '',
                'MachineID': '',
                'SeatNum': 0,
                'CoinRatio': 0,
                'CustomData': coin_custom_data,
                'PublishVer': sender_publish_ver_result,
            }
            detail_coin.append(c1)

            c2 = {
                'ProDate': pro_date,
                'TransferTs': transfer_ts_result,
                'TransferID': transfer_id_result,
                'SenderUserID': sender_user_id_result,
                'SenderNickname': sender_nickname_result,
                'SenderVipLV': sender_vip_lv_result,
                'ReceiverUserID': receiver_user_id_result,
                'ReceiverNickname': receiver_nickname_result,
                'CoinID': coin_id_result,
                'Amount': convert_large_numbers(coin_amount_result + bonus_coin_amount_result, self.large_numbers),
                'SenderCoinBefore': coin_balance_result,
                'SenderCoinAfter': convert_large_numbers(end_coin_balance_result, self.large_numbers),
                'CustomData': coin_custom_data,
            }
            transfer_coin.append(c2)

        dic_name = 'receiver_coin_list.'
        for coin in receiver_coin_list:

            coin_id_err, coin_id_result = str_valid(fun_name, dic_name + 'coin_id', coin.get('coin_id'), '',
                                                    allow_none=False)
            if coin_id_err is not None:
                self.logger.warn(coin_id_err + ' and required')
                continue

            if self.check_attributes:
                coin_amount_err, coin_amount_result = float_valid(fun_name, dic_name + 'coin_amount',
                                                                  coin.get('coin_amount'), 0, allow_none=False)
                if coin_amount_err is not None:
                    self.logger.info(coin_amount_err)

                bonus_coin_amount_err, bonus_coin_amount_result = float_valid(fun_name, dic_name + 'bonus_coin_amount',
                                                                              coin.get('bonus_coin_amount'), 0,
                                                                              allow_none=True)
                if bonus_coin_amount_err is not None:
                    self.logger.info(bonus_coin_amount_err)

                end_coin_balance_err, end_coin_balance_result = float_valid(fun_name, dic_name + 'end_coin_balance',
                                                                            coin.get('end_coin_balance'), 0,
                                                                            allow_none=False)
                if end_coin_balance_err is not None:
                    self.logger.info(end_coin_balance_err)
            else:
                coin_amount_result = coin.get('coin_amount', 0)
                bonus_coin_amount_result = coin.get('bonus_coin_amount', 0)
                end_coin_balance_result = coin.get('end_coin_balance', 0)

            coin_custom_data = str_valid_base(coin.get('custom_data', ''), '', replace_special_word=False)

            c3 = {
                'ProDate': pro_date,
                'ChangeTs': transfer_ts_result,
                'UserID': receiver_user_id_result,
                'Nickname': receiver_nickname_result,
                'CoinID': coin_id_result,
                'IncreaseCoins': 0 if positive_valid(coin_amount_result) is False else convert_large_numbers(
                    coin_amount_result, self.large_numbers),
                'DecreaseCoins': 0 if negative_valid(coin_amount_result) is False else convert_large_numbers(
                    coin_amount_result, self.large_numbers),
                'BonusCoins': convert_large_numbers(bonus_coin_amount_result, self.large_numbers),
                'CoinBalanceAfter': convert_large_numbers(end_coin_balance_result, self.large_numbers),
                'ChangeReason': change_reason,
                'SceneState': '',
                'GameID': '',
                'RoomID': '',
                'MachineID': '',
                'SeatNum': 0,
                'CoinRatio': 0,
                'CustomData': coin_custom_data,
                'PublishVer': receiver_publish_ver_result,
            }
            detail_coin.append(c3)

        detail_item = list()
        transfer_item = list()
        dic_name = 'sender_item_list.'
        for item in sender_item_list:

            item_id_err, item_id_result = str_valid(fun_name, dic_name + 'item_id', item.get('item_id'), '',
                                                    allow_none=False)
            if item_id_err is not None:
                self.logger.warn(item_id_err + ' and required')
                continue

            if self.check_attributes:
                item_amount_err, item_amount_result = float_valid(fun_name, dic_name + 'item_amount',
                                                                  item.get('item_amount'), 0, allow_none=False)
                if item_amount_err is not None:
                    self.logger.info(item_amount_err)

                bonus_item_amount_err, bonus_item_amount_result = float_valid(fun_name, dic_name + 'bonus_item_amount',
                                                                              item.get('bonus_item_amount'), 0,
                                                                              allow_none=True)
                if bonus_item_amount_err is not None:
                    self.logger.info(bonus_item_amount_err)

                coin_value_err, coin_value_result = float_valid(fun_name, dic_name + 'coin_value',
                                                                item.get('coin_value'), 0, allow_none=False)
                if coin_value_err is not None:
                    self.logger.info(coin_value_err)

                end_item_balance_err, end_item_balance_result = float_valid(fun_name, dic_name + 'end_item_balance',
                                                                            item.get('end_item_balance'), 0,
                                                                            allow_none=False)
                if end_item_balance_err is not None:
                    self.logger.info(end_item_balance_err)

                available_sec_err, available_sec_result = int_valid(fun_name, dic_name + 'available_sec',
                                                                    item.get('available_sec'), 0, allow_none=True)
                if available_sec_err is not None:
                    self.logger.info(available_sec_err)
            else:
                item_amount_result = item.get('item_amount', 0)
                bonus_item_amount_result = item.get('bonus_item_amount', 0)
                coin_value_result = item.get('coin_value', 0)
                end_item_balance_result = item.get('end_item_balance', 0)
                available_sec_result = item.get('available_sec', 0)

            item_custom_data = str_valid_base(item.get('custom_data', ''), '', replace_special_word=False)
            item_balance_err, item_balance_result = float_valid(fun_name, dic_name + 'item_balance',
                                                                item.get('item_balance'), None, allow_none=True)
            if item_balance_err is not None:
                self.logger.info(item_balance_err)
            if item_balance_result is None:
                val = end_item_balance_result - bonus_item_amount_result - item_amount_result
                item_balance_result = convert_large_numbers(val, self.large_numbers)

            i1 = {
                'ProDate': pro_date,
                'ChangeTs': transfer_ts_result,
                'UserID': sender_user_id_result,
                'Nickname': sender_nickname_result,
                'ItemID': item_id_result,
                'AvailableSec': available_sec_result,
                'CoinValue': convert_large_numbers(coin_value_result, self.large_numbers),
                'ChangeAmount': convert_large_numbers(item_amount_result, self.large_numbers),
                'BonusAmount': convert_large_numbers(bonus_item_amount_result, self.large_numbers),
                'ItemBalanceAfter': convert_large_numbers(end_item_balance_result, self.large_numbers),
                'ChangeReason': change_reason,
                'CoinRatio': 0,
                'SceneState': '',
                'GameID': '',
                'RoomID': '',
                'MachineID': '',
                'SeatNum': 0,
                'CustomData': item_custom_data,
                'PublishVer': sender_publish_ver_result,
            }
            detail_item.append(i1)

            i2 = {
                'ProDate': pro_date,
                'TransferTs': transfer_ts_result,
                'TransferID': transfer_id_result,
                'SenderUserID': sender_user_id_result,
                'SenderNickname': sender_nickname_result,
                'SenderVipLV': sender_vip_lv_result,
                'ReceiverUserID': receiver_user_id_result,
                'ReceiverNickname': receiver_nickname_result,
                'ItemID': item_id_result,
                'Amount': convert_large_numbers(item_amount_result + bonus_item_amount_result, self.large_numbers),
                'AvailableSec': available_sec_result,
                'CoinValue': convert_large_numbers(coin_value_result, self.large_numbers),
                'SenderItemBefore': item_balance_result,
                'SenderItemAfter': convert_large_numbers(end_item_balance_result, self.large_numbers),
                'CustomData': item_custom_data,
            }
            transfer_item.append(i2)

        dic_name = 'receiver_item_list.'
        for item in receiver_item_list:

            item_id_err, item_id_result = str_valid(fun_name, dic_name + 'item_id', item.get('item_id'), '',
                                                    allow_none=False)
            if item_id_err is not None:
                self.logger.warn(item_id_err + ' and required')
                continue

            if self.check_attributes:
                item_amount_err, item_amount_result = float_valid(fun_name, dic_name + 'item_amount',
                                                                  item.get('item_amount'), 0, allow_none=False)
                if item_amount_err is not None:
                    self.logger.info(item_amount_err)

                bonus_item_amount_err, bonus_item_amount_result = float_valid(fun_name, dic_name + 'bonus_item_amount',
                                                                              item.get('bonus_item_amount'), 0,
                                                                              allow_none=True)
                if bonus_item_amount_err is not None:
                    self.logger.info(bonus_item_amount_err)

                coin_value_err, coin_value_result = float_valid(fun_name, dic_name + 'coin_value',
                                                                item.get('coin_value'), 0, allow_none=False)
                if coin_value_err is not None:
                    self.logger.info(coin_value_err)

                end_item_balance_err, end_item_balance_result = float_valid(fun_name, dic_name + 'end_item_balance',
                                                                            item.get('end_item_balance'), 0,
                                                                            allow_none=False)
                if end_item_balance_err is not None:
                    self.logger.info(end_item_balance_err)

                available_sec_err, available_sec_result = int_valid(fun_name, dic_name + 'available_sec',
                                                                    item.get('available_sec'), 0, allow_none=True)
                if available_sec_err is not None:
                    self.logger.info(available_sec_err)
            else:
                item_amount_result = item.get('item_amount', 0)
                bonus_item_amount_result = item.get('bonus_item_amount', 0)
                coin_value_result = item.get('coin_value', 0)
                end_item_balance_result = item.get('end_item_balance', 0)
                available_sec_result = item.get('available_sec', 0)

            item_custom_data = str_valid_base(item.get('custom_data', ''), '', replace_special_word=False)

            i3 = {
                'ProDate': pro_date,
                'ChangeTs': transfer_ts_result,
                'UserID': receiver_user_id_result,
                'Nickname': receiver_nickname_result,
                'ItemID': item_id_result,
                'AvailableSec': available_sec_result,
                'CoinValue': convert_large_numbers(coin_value_result, self.large_numbers),
                'ChangeAmount': convert_large_numbers(item_amount_result, self.large_numbers),
                'BonusAmount': convert_large_numbers(bonus_item_amount_result, self.large_numbers),
                'ItemBalanceAfter': convert_large_numbers(end_item_balance_result, self.large_numbers),
                'ChangeReason': change_reason,
                'CoinRatio': 0,
                'SceneState': '',
                'GameID': '',
                'RoomID': '',
                'MachineID': '',
                'SeatNum': 0,
                'CustomData': item_custom_data,
                'PublishVer': receiver_publish_ver_result,
            }
            detail_item.append(i3)

        for i in detail_coin:
            self.ark_data_manager.send('DetailCoin', i, transfer_ts_result, callback=self.callback,
                                       mongo_coll_date=self.get_dt_mongo_coll_date(dt))
        for j in transfer_coin:
            self.ark_data_manager.send('TransferCoin', j, transfer_ts_result, callback=self.callback,
                                       mongo_coll_date=self.get_dt_mongo_coll_date(dt))

        for m in detail_item:
            self.ark_data_manager.send('DetailItem', m, transfer_ts_result, callback=self.callback,
                                       mongo_coll_date=self.get_dt_mongo_coll_date(dt))
        for n in transfer_item:
            self.ark_data_manager.send('TransferItem', n, transfer_ts_result, callback=self.callback,
                                       mongo_coll_date=self.get_dt_mongo_coll_date(dt))

        return True

    # 9.離開遊戲：SessionLength
    def exit_app(self, udid, play_start_ts, play_end_ts, last_play_start_ts, sys_type, sys_ver, country, curr_channel,
                 publish_ver, lv, vip_lv, end_lv, end_vip_lv, user_id=None, appsflyer_id=None, region=None, city=None,
                 role_id=None, role_nickname=None, dev=None, mac=None, android_id=None, aaid=None, imei=None, idfa=None,
                 idfv=None, fid=None, ip=None, network=None, longitude=None, latitude=None, nickname=None, channel=None,
                 install_source=None, logo_id=None, logo_name=None, kiosk_id=None, kiosk_name=None, time_zone=None,
                 lang=None, custom_data=None):
        """
        :param str udid: 裝置ID
        :param float play_start_ts: 啟動遊戲時間time.time()
        :param float play_end_ts: 退出遊戲時間time.time()
        :param float last_play_start_ts: 上次啟動遊戲時間time.time()
        :param str sys_type: 操作系統
        :param str sys_ver: 操作系統版本
        :param str country: 所在地區(國別)
        :param str curr_channel: 玩家登入渠道
        :param str publish_ver: 遊戲版本
        :param int lv: 開始等級
        :param int vip_lv: 開始VIP等級
        :param int end_lv: 結束等級
        :param int end_vip_lv: 結束VIP等級
        :param int user_id: 玩家ID帳號, 允許None
        :param str appsflyer_id: 媒體下載歸因裝置ID, 允許None
        :param str region: 所在地區省州, 允許None
        :param str city: 所在地區城市, 允許None
        :param str role_id: 玩家角色ID, 允許None
        :param str role_nickname: 玩家角色暱稱, 允許None
        :param str dev: 機型, 允許None
        :param str mac: 裝置MAC,允許None
        :param str android_id: 允許None
        :param str aaid: 允許None
        :param str imei: 允許None
        :param str idfa: 允許None
        :param str idfv: 允許None
        :param str fid: 允許None
        :param str ip: 玩家登入IP, 允許None
        :param str network: 聯網方式, 允許None
        :param float longitude: 經度, 允許None
        :param float latitude: 緯度, 允許None
        :param str nickname: 暱稱,允許None
        :param str channel: 下載平台渠道,允許None
        :param str install_source: 第三方媒體首次安裝來源, 允許None
        :param int logo_id: 代理商ID, 允許None
        :param str logo_name: 允許None
        :param int kiosk_id: 店家ID, 允許None
        :param str kiosk_name: 允許None
        :param float time_zone: 玩家時區, 允許None
        :param str lang: 使用語系, 允許None
        :param str custom_data: 自訂擴充定義與值,允許None
        """
        fun_name = 'exit_app'

        udid_err, udid_result = str_valid(fun_name, 'udid', udid, '', allow_none=False)
        if udid_err is not None:
            self.logger.error(udid_err + ' and required')
            return False

        play_start_ts_err, play_start_ts_result = timestamp_valid(fun_name, 'play_start_ts', play_start_ts,
                                                                  allow_none=False)
        if play_start_ts_err is not None:
            self.logger.error(play_start_ts_err)
            return False

        play_end_ts_err, play_end_ts_result = timestamp_valid_base(fun_name, 'play_end_ts', play_end_ts,
                                                                   allow_none=False)
        if play_end_ts_err is not None:
            self.logger.warn(play_end_ts_err)

        last_play_start_ts_err, last_play_start_ts_result = timestamp_valid_base(fun_name, 'last_play_start_ts',
                                                                                 last_play_start_ts, allow_none=False)
        if last_play_start_ts_err is not None:
            self.logger.warn(last_play_start_ts_err)

        if self.check_attributes:

            sys_type_err, sys_type_result = str_valid(fun_name, 'sys_type', sys_type, '', allow_none=False)
            if sys_type_err is not None:
                self.logger.warn(sys_type_err)

            sys_ver_err, sys_ver_result = str_valid(fun_name, 'sys_ver', sys_ver, '', allow_none=False)
            if sys_ver_err is not None:
                self.logger.warn(sys_ver_err)

            country_err, country_result = str_valid(fun_name, 'country', country, '', allow_none=False)
            if country_err is not None:
                self.logger.warn(country_err)

            curr_channel_err, curr_channel_result = str_valid(fun_name, 'curr_channel', curr_channel, '',
                                                              allow_none=False)
            if curr_channel_err is not None:
                self.logger.warn(curr_channel_err)

            publish_ver_err, publish_ver_result = str_valid(fun_name, 'publish_ver', publish_ver, '', allow_none=False)
            if publish_ver_err is not None:
                self.logger.warn(publish_ver_err)

            lv_err, lv_result = int_valid(fun_name, 'lv', lv, 0, allow_none=False)
            if lv_err is not None:
                self.logger.warn(lv_err)

            vip_lv_err, vip_lv_result = int_valid(fun_name, 'vip_lv', vip_lv, 0, allow_none=False)
            if vip_lv_err is not None:
                self.logger.warn(vip_lv_err)

            end_lv_err, end_lv_result = int_valid(fun_name, 'end_lv', end_lv, 0, allow_none=False)
            if end_lv_err is not None:
                self.logger.warn(end_lv_err)

            end_vip_lv_err, end_vip_lv_result = int_valid(fun_name, 'end_vip_lv', end_vip_lv, 0, allow_none=False)
            if end_vip_lv_err is not None:
                self.logger.warn(end_vip_lv_err)

            user_id_err, user_id_result = int_valid(fun_name, 'user_id', user_id, 0, allow_none=True)
            if user_id_err is not None:
                self.logger.info(user_id_err)

            longitude_err, longitude_result = longitude_valid(fun_name, 'longitude', longitude, -999, allow_none=True)
            if longitude_err is not None:
                self.logger.info(longitude_err)

            latitude_err, latitude_result = latitude_valid(fun_name, 'latitude', latitude, -999, allow_none=True)
            if latitude_err is not None:
                self.logger.info(latitude_err)

            logo_id_err, logo_id_result = int_valid(fun_name, 'logo_id', logo_id, 0, allow_none=True)
            if logo_id_err is not None:
                self.logger.info(logo_id_err)

            kiosk_id_err, kiosk_id_result = int_valid(fun_name, 'kiosk_id', kiosk_id, 0, allow_none=True)
            if kiosk_id_err is not None:
                self.logger.info(kiosk_id_err)

            time_zone_err, time_zone_result = float_valid(fun_name, 'time_zone', time_zone, -999, allow_none=True)
            if time_zone_err is not None:
                self.logger.info(time_zone_err)
        else:
            sys_type_result = str_valid_base(sys_type, '')
            sys_ver_result = str_valid_base(sys_ver, '')
            country_result = str_valid_base(country, '')
            curr_channel_result = str_valid_base(curr_channel, '')
            publish_ver_result = str_valid_base(publish_ver, '')
            lv_result = 0 if lv is None else lv
            vip_lv_result = 0 if vip_lv is None else vip_lv
            end_lv_result = 0 if end_lv is None else end_lv
            end_vip_lv_result = 0 if end_vip_lv is None else end_vip_lv
            user_id_result = 0 if user_id is None else user_id
            longitude_result = -999 if longitude is None else longitude
            latitude_result = -999 if latitude is None else latitude
            logo_id_result = 0 if logo_id is None else logo_id
            kiosk_id_result = 0 if kiosk_id is None else kiosk_id
            time_zone_result = -999 if time_zone is None else time_zone

        play_length_sec = 0 if play_end_ts_result == 0 or play_start_ts_result == 0 else int(
            (play_end_ts_result - play_start_ts_result) / 1000000)
        play_length_sec = 0 if play_length_sec < 1 else play_length_sec
        intervals = 0 if last_play_start_ts_result == 0 else int(
            (play_start_ts_result - last_play_start_ts_result) / 1000000)
        intervals = 0 if intervals < 1 else intervals
        dt = self.get_logger_datetime(play_end_ts_result)
        sl = {
            'ProDate': self.get_dt_pro_date(dt),
            'PlayStartTs': play_start_ts_result,
            'PlayEndTs': play_end_ts_result,
            'UserID': user_id_result,
            'Nickname': str_valid_base(nickname, '', replace_special_word=True),
            'UDID': udid_result,
            'AppsflyerID': str_valid_base(appsflyer_id, ''),
            'PlayLengthSec': play_length_sec,
            'Intervals': intervals,
            'SysType': sys_type_result,
            'SysVer': sys_ver_result,
            'Country': country_result,
            'Region': str_valid_base(region, ''),
            'City': str_valid_base(city, ''),
            'Channel': str_valid_base(channel, ''),
            'CurrChannel': curr_channel_result,
            'InstallSource': str_valid_base(install_source, ''),
            'PublishVer': publish_ver_result,
            'LV': lv_result,
            'VipLV': vip_lv_result,
            'EndLV': end_lv_result,
            'EndVipLV': end_vip_lv_result,
            'LogoID': logo_id_result,
            'LogoName': str_valid_base(logo_name, ''),
            'KioskID': kiosk_id_result,
            'KioskName': str_valid_base(kiosk_name, ''),
            'RoleID': str_valid_base(role_id, ''),
            'RoleNickname': str_valid_base(role_nickname, '', replace_special_word=True),
            'DEV': str_valid_base(dev, ''),
            'MAC': str_valid_base(mac, ''),
            'AndroidID': str_valid_base(android_id, ''),
            'AAID': str_valid_base(aaid, ''),
            'IMEI': str_valid_base(imei, ''),
            'IDFA': str_valid_base(idfa, ''),
            'IDFV': str_valid_base(idfv, ''),
            'FID': str_valid_base(fid, ''),
            'IP': str_valid_base(ip, ''),
            'Network': str_valid_base(network, ''),
            'Longitude': longitude_result,
            'Latitude': latitude_result,
            'TimeZone': time_zone_result,
            'Lang': str_valid_base(lang, ''),
            'CustomData': str_valid_base(custom_data, '', replace_special_word=False),
        }
        self.ark_data_manager.send("SessionLength", sl, play_start_ts_result, callback=self.callback,
                                   mongo_coll_date=self.get_dt_mongo_coll_date(dt))
        return True

    # 10.回報Client錯誤資訊：ClientErrorLog
    def report_error(self, code_name, user_id, session_id, event_id, dict_message, user_from_id=None, game_id=None,
                     scene_state=None, channel=None, udid=None, dev=None, sys_type=None, sys_ver=None, lang=None,
                     publish_ver=None, country=None, ip=None, ip_country=None, ip_region=None, ip_city=None,
                     create_ts=None):
        """
        :param str code_name: 專案代號
        :param int user_id: 玩家ID帳號
        :param str session_id: session_id
        :param int event_id: 事件ID
        :param dict dict_message: 除錯需記錄的資訊
        :param int user_from_id: 玩家來源ID, 玩家從別的來源來的Id, 允許None
        :param int game_id: 遊戲ID, 允許None
        :param str scene_state: 場景名稱, 允許None
        :param str channel: 下載平台渠道, 允許None
        :param str udid: 裝置識別ID, 允許None
        :param str dev: 裝置型號, 允許None
        :param str sys_type: 裝置OS, 允許None
        :param str sys_ver: 作業系統版本, 允許None
        :param str lang: 使用語系, 允許None
        :param str publish_ver: 遊戲版本, 允許None
        :param str country: 所在地區(國別), 允許None
        :param str ip: 玩家登入IP, 允許None
        :param str ip_country: 玩家當時的國家, 允許None
        :param str ip_region: 玩家當時的州別, 允許None
        :param str ip_city: 玩家當時的城市, 允許None
        :param float create_ts: 事件發生時間time.time(), 允許None, 若None系統會用現在時間
        """
        fun_name = 'report_error'

        user_id_err, user_id_result = int_valid(fun_name, 'user_id', user_id, 0, allow_none=False)
        if user_id_err is not None:
            self.logger.error(user_id_err + ' and required')
            return False

        event_id_err, event_id_result = int_valid(fun_name, 'event_id', event_id, 0, allow_none=False)
        if event_id_err is not None:
            self.logger.error(event_id_err + ' and required')
            return False

        if type(dict_message) is not dict:
            self.logger.warn('%s: dict_message is %s it must be dict' % (fun_name, dict_message))
            dict_message = dict()

        create_ts_err, create_ts_result = timestamp_valid(fun_name, 'create_ts', create_ts, allow_none=True)
        if create_ts_err is not None:
            self.logger.info(create_ts_err)

        if self.check_attributes:
            code_name_err, code_name_result = str_valid(fun_name, 'code_name', code_name, '', allow_none=False)
            if code_name_err is not None:
                self.logger.warn(code_name_err)

            session_id_err, session_id_result = str_valid(fun_name, 'session_id', session_id, '', allow_none=False)
            if session_id_err is not None:
                self.logger.warn(session_id_err)

            user_from_id_err, user_from_id_result = int_valid(fun_name, 'user_from_id', user_from_id, 0,
                                                              allow_none=True)
            if user_from_id_err is not None:
                self.logger.info(user_from_id_err)

            game_id_err, game_id_result = int_valid(fun_name, 'game_id', game_id, 0, allow_none=True)
            if game_id_err is not None:
                self.logger.info(game_id_err)
        else:
            code_name_result = str_valid_base(code_name, '')
            session_id_result = str_valid_base(session_id, '')
            user_from_id_result = 0 if user_from_id is None else user_from_id
            game_id_result = 0 if game_id is None else game_id
        dt = self.get_logger_datetime(create_ts_result)
        ce = {
            'ProDate': self.get_dt_pro_date(dt),
            'CreateTs': create_ts_result,
            'CodeName': code_name_result,
            'SessionID': session_id_result,
            'UserID': user_id_result,
            'UserFromID': user_from_id_result,
            'EventID': event_id_result,
            'GameID': game_id_result,
            'SceneState': str_valid_base(scene_state, ''),
            'Channel': str_valid_base(channel, ''),
            'UDID': str_valid_base(udid, ''),
            'DEV': str_valid_base(dev, ''),
            'SysType': str_valid_base(sys_type, ''),
            'SysVer': str_valid_base(sys_ver, ''),
            'Lang': str_valid_base(lang, ''),
            'PublishVer': str_valid_base(publish_ver, ''),
            'Country': str_valid_base(country, ''),
            'IP': str_valid_base(ip, ''),
            'IPCountry': str_valid_base(ip_country, ''),
            'IPRegion': str_valid_base(ip_region, ''),
            'IPCity': str_valid_base(ip_city, ''),
            'Message': dict_message,
        }
        self.ark_data_manager.send("ClientErrorLog", ce, create_ts_result, callback=self.callback,
                                   mongo_coll_date=self.get_dt_mongo_coll_date(dt))
        return True

    # 11.更新玩家基本資料：PlayerProfile
    def update_profile(self, user_id, nickname=None, email=None, device_email=None, gender=None, birthdate=None,
                       custom_birthdate=None, open_type=None, open_id=None, open_nickname=None, install_source=None,
                       install_ts=None, custom_country=None, channel=None, lang=None, custom_data=None, update_ts=None,
                       account_create_ts=None):
        """
        :param int user_id: 玩家ID帳號
        :param str nickname: 暱稱, 允許None
        :param str email: 玩家電子信箱, 允許None
        :param str device_email: 玩家裝置電子信箱, 允許None
        :param str gender: 性别, 允許None
        :param str birthdate: 生日, YYYY-MM-DD, 允許None
        :param str custom_birthdate: 玩家自訂生日, YYYY-MM-DD, 允許None
        :param str open_type: 第三方驗證者, 允許None
        :param str open_id: 第三方驗證使用者ID, 允許None
        :param str open_nickname: 第三方驗證使用者暱稱, 允許None
        :param str install_source: 媒體來源, 允許None
        :param float install_ts: 媒體安裝時間time.time(), 允許None, 若None為0
        :param str custom_country: 玩家自訂國家, 允許None
        :param str channel: 下載平台渠道, 允許None
        :param str lang: 使用語系, 允許None
        :param str custom_data: 自訂擴充定義與值,允許None
        :param float update_ts: 資料更新時間time.time(), 允許None, 若None系統會用現在時間
        :param account_create_ts: 帳號建立時間time.time(), 允許None(0)
        """
        fun_name = 'update_profile'

        user_id_err, user_id_result = int_valid(fun_name, 'user_id', user_id, 0, allow_none=False)
        if user_id_err is not None:
            self.logger.error(user_id_err + ' and required')
            return False

        update_ts_err, update_ts_result = timestamp_valid(fun_name, 'update_ts', update_ts, allow_none=True)
        if update_ts_err is not None:
            self.logger.info(update_ts_err)

        if self.check_attributes:
            birthdate_err, birthdate_result = date_valid(fun_name, 'birthdate', birthdate, '1970-01-01',
                                                         allow_none=True)
            if birthdate_err is not None:
                self.logger.info(birthdate_err)

            custom_birthdate_err, custom_birthdate_result = date_valid(fun_name, 'custom_birthdate', custom_birthdate,
                                                                       '1970-01-01', allow_none=True)
            if custom_birthdate_err is not None:
                self.logger.info(custom_birthdate_err)

            install_ts_err, install_ts_result = timestamp_valid_base(fun_name, 'install_ts', install_ts,
                                                                     allow_none=True)
            if install_ts_err is not None:
                self.logger.info(install_ts_err)
            account_create_err, account_create_ts_result = timestamp_valid(fun_name, 'account_create_ts',
                                                                           account_create_ts, allow_none=True)
            if account_create_err is not None:
                self.logger.info(account_create_err)

        else:
            birthdate_err, birthdate_result = date_valid(fun_name, 'birthdate', birthdate, '1970-01-01',
                                                         allow_none=True)
            custom_birthdate_err, custom_birthdate_result = date_valid(fun_name, 'custom_birthdate', custom_birthdate,
                                                                       '1970-01-01', allow_none=True)
            install_ts_err, install_ts_result = timestamp_valid_base(fun_name, 'install_ts', install_ts,
                                                                     allow_none=True)
            account_create_ts_err, account_create_ts_result = timestamp_valid_base(fun_name, 'account_create_ts',
                                                                                   account_create_ts, allow_none=True)
        dt = self.get_logger_datetime(update_ts_result)
        pp = {
            'ProDate': self.get_dt_pro_date(dt),
            'UserID': user_id_result,
            'UpdateTs': update_ts_result,
            'Nickname': str_valid_base(nickname, '', replace_special_word=True),
            'Gender': str_valid_base(gender, ''),
            'Birthdate': birthdate_result,
            'CustomBirthdate': custom_birthdate_result,
            'Email': str_valid_base(email, ''),
            'DeviceEmail': str_valid_base(device_email, ''),
            'OpenType': str_valid_base(open_type, ''),
            'OpenID': str_valid_base(open_id, ''),
            'OpenNickname': str_valid_base(open_nickname, '', replace_special_word=True),
            'InstallSource': str_valid_base(install_source, ''),
            'InstallTs': install_ts_result,
            'CustomCountry': str_valid_base(custom_country, ''),
            'Channel': str_valid_base(channel, ''),
            'Lang': str_valid_base(lang, ''),
            'CustomData': str_valid_base(custom_data, '', replace_special_word=False),
            'AccountCreateTs': account_create_ts_result,
        }
        self.ark_data_manager.send("PlayerProfile", pp, update_ts_result, callback=self.callback,
                                   mongo_coll_date=self.get_dt_mongo_coll_date(dt))
        return True

    # 12.玩家權限異動：PermissionChange
    def change_permission(self, user_id, event_reason, event_id, user_status, start_ts=None, end_ts=None,
                          event_value=None, event_string=None, operator=None, nickname=None, coin_balance=None, lv=None,
                          vip_lv=None, custom_data=None, change_ts=None):
        """
        :param int user_id: 玩家ID帳號
        :param str event_reason: 異動事件原因
        :param int event_id: 異動目標事件id
        :param str user_status: 異動後帳號狀態, 停權、正常
        :param int start_ts: 異動生效起始時間time.time(), 允許None, 若None系統會用現在時間
        :param int end_ts: 異動生效結束時間time.time(), 允許None, 若None系統會用現在時間+100年
        :param int event_value: 異動事件數值, 人氣值、群組人數、重置頭像次數, 允許None
        :param str event_string: 異動事件字串, 供客服使用寫入Splunk不需轉檔分析, 允許None
        :param str operator: 操作者, 允許None
        :param str nickname: 暱稱, 允許None
        :param float,int coin_balance: 玩家剩餘金幣, 允許None
        :param int lv: 等級, 允許None
        :param int vip_lv: VIP等級, 允許None
        :param str custom_data: 自訂擴充定義與值,允許None
        :param float change_ts: 建立時間time.time(), 允許None, 若None系統會用現在時間
        """
        fun_name = 'change_permission'

        user_id_err, user_id_result = int_valid(fun_name, 'user_id', user_id, 0, allow_none=False)
        if user_id_err is not None:
            self.logger.error(user_id_err + ' and required')
            return False

        event_id_err, event_id_result = int_valid(fun_name, 'event_id', event_id, 0, allow_none=False)
        if event_id_err is not None:
            self.logger.error(event_id_err + ' and required')
            return False

        start_ts_err, start_ts_result = timestamp_valid(fun_name, 'start_ts', start_ts, allow_none=True)
        if start_ts_err is not None:
            self.logger.info(start_ts_err)

        end_ts_err, end_ts_result = timestamp_valid_base(fun_name, 'end_ts', end_ts, allow_none=True)
        if end_ts_err is not None:
            self.logger.info(end_ts_err)
        if end_ts_result == 0:
            # 100年後 (60 * 60 * 24 * 365 * 100) * 1000000
            end_ts_result = start_ts_result + 3153600000000000

        change_ts_err, change_ts_result = timestamp_valid(fun_name, 'change_ts', change_ts, allow_none=True)
        if change_ts_err is not None:
            self.logger.info(change_ts_err)

        if self.check_attributes:
            event_reason_err, event_reason_result = str_valid(fun_name, 'event_reason', event_reason, '',
                                                              allow_none=False)
            if event_reason_err is not None:
                self.logger.warn(event_reason_err)

            user_status_err, user_status_result = str_valid(fun_name, 'user_status', user_status, '',
                                                            allow_none=False)
            if user_status_err is not None:
                self.logger.warn(user_status_err)

            event_value_err, event_value_result = int_valid(fun_name, 'event_value', event_value, 0, allow_none=True)
            if event_value_err is not None:
                self.logger.info(event_value_err)

            coin_balance_err, coin_balance_result = float_valid(fun_name, 'coin_balance', coin_balance, 0,
                                                                allow_none=True)
            if coin_balance_err is not None:
                self.logger.info(coin_balance_err)

            lv_err, lv_result = int_valid(fun_name, 'lv', lv, 0, allow_none=True)
            if lv_err is not None:
                self.logger.info(lv_err)

            vip_lv_err, vip_lv_result = int_valid(fun_name, 'vip_lv', vip_lv, 0, allow_none=True)
            if vip_lv_err is not None:
                self.logger.info(vip_lv_err)
        else:
            event_reason_result = str_valid_base(event_reason, '')
            user_status_result = str_valid_base(user_status, '')
            event_value_result = 0 if event_value is None else event_value
            coin_balance_result = 0 if coin_balance is None else coin_balance
            lv_result = 0 if lv is None else lv
            vip_lv_result = 0 if vip_lv is None else vip_lv
        dt = self.get_logger_datetime(change_ts_result)
        pc = {
            'ProDate': self.get_dt_pro_date(dt),
            'CreateTs': change_ts_result,
            'UserID': user_id_result,
            'Nickname': str_valid_base(nickname, '', replace_special_word=True),
            'CoinBalance': convert_large_numbers(coin_balance_result, self.large_numbers),
            'LV': lv_result,
            'VipLV': vip_lv_result,
            'EventID': event_id_result,
            'StartTs': start_ts_result,
            'EndTs': end_ts_result,
            'EventValue': event_value_result,
            'EventString': str_valid_base(event_string, ''),
            'EventReason': event_reason_result,
            'UserStatus': user_status_result,
            'Operator': str_valid_base(operator, ''),
            'CustomData': str_valid_base(custom_data, '', replace_special_word=False),
        }
        self.ark_data_manager.send("PermissionChange", pc, change_ts_result, callback=self.callback,
                                   mongo_coll_date=self.get_dt_mongo_coll_date(dt))
        return True

    # 13.回報Game Server整點資訊(game server使用socket連線方式, 需記錄整體連線數)：HourlyStats
    def report_server_stats(self, stats_date, server_id, hour_id, ccu):
        """
        :param str stats_date: 資料收集日期, YYYY-MM-DD
        :param str server_id: 遊戲伺服器ID
        :param int hour_id: 整點, 0~23
        :param int ccu: 整點在線人數
        """
        fun_name = 'report_server_stats'

        stats_date_err, stats_date_result = date_valid(fun_name, 'stats_date', stats_date, '1970-01-01',
                                                       allow_none=False)
        if stats_date_err is not None:
            self.logger.error(stats_date_err + ' and required')
            return False

        server_id_err, server_id_result = str_valid(fun_name, 'server_id', server_id, '', allow_none=False)
        if server_id_err is not None:
            self.logger.warn(server_id_err + ' and required')

        hour_id_err, hour_id_result = int_valid(fun_name, 'hour_id', hour_id, 0, allow_none=False)
        if hour_id_err is not None:
            self.logger.error(hour_id_err + ' and required')
            return False

        if self.check_attributes:
            ccu_err, ccu_result = int_valid(fun_name, 'ccu', ccu, 0, allow_none=False)
            if ccu_err is not None:
                self.logger.warn(ccu_err)
        else:
            ccu_result = 0 if ccu is None else ccu
        dt = self.get_logger_datetime()
        hs = {
            'ProDate': self.get_dt_pro_date(dt),
            'StatsDate': stats_date_result,
            'ServerID': server_id_result,
            'HourTime': hour_id_result,
            'CCU': ccu_result,
        }
        create_ts = int(time.time() * 1000000)
        self.ark_data_manager.send("HourlyStats", hs, create_ts, callback=self.callback,
                                   mongo_coll_date=self.get_dt_mongo_coll_date(dt))
        return True

    # 14.遊戲回合結算：DetailBetWin
    def bet_win(self, user_id, create_ts, lv=0, vip_lv=0, curr_channel="", rtp_ver="", standard_total_bet=0,
                standard_total_win=0, game_type="", room_id="", game_id="", fish_id="", machine_type="", coin_ratio=0,
                single_bet=0, bet_lines=0, machine_id="", seat_num=0, bet_coin_id=0, bet_coin_balance_after=0,
                win_coin_id=0, win_coin_balance_after=0, total_bet=0, total_win=0, net_bet=0, net_win=0,
                max_qualify_jp_type=0, jackpot_id="", jackpot_type="", jackpot_win=0, jackpot_win_type=0,
                jackpot_rtp_ver=0, item_id=0, buffer_bet=0, buffer_win=0, item_win=0, extra_bet=0, free_game_times=0,
                feature_game_type="", udid="", custom_data="", session_id=""):
        """

        @param user_id: int 玩家ID(帳號)
        @param create_ts: int 遊玩時間
        @param lv: int 等級
        @param vip_lv: int VIP等級
        @param curr_channel: str 玩家登入渠道
        @param rtp_ver: str 機率版本
        @param standard_total_bet: float 標準總押注
        @param standard_total_win: float 標準總贏分
        @param game_type: str 遊戲類別
        @param room_id: str 遊戲廳館ID
        @param game_id: str 遊戲ID
        @param fish_id: str 魚種ID
        @param machine_type: str 機台分類
        @param coin_ratio: float 機台換算幣值
        @param single_bet: float 單次押注金額
        @param bet_lines: int 押注線數
        @param machine_id: str 遊戲機台ID
        @param seat_num: str 座位編號
        @param bet_coin_id: str 押注使用虛擬幣
        @param bet_coin_balance_after: float 押注使用虛擬幣餘額
        @param win_coin_id: str 贏分可得虛擬幣
        @param win_coin_balance_after: float 贏分可得虛擬幣餘額
        @param total_bet: float 原始總押注
        @param total_win: float 原始總贏分
        @param net_bet: float 淨押分
        @param net_win: float 淨贏分
        @param max_qualify_jp_type: int 是否參與Jackpot
        @param jackpot_id: str 遊戲內JP代碼
        @param jackpot_type: str 遊戲內JP累積押分類型
        @param jackpot_win: float 遊戲內JP總贏分
        @param jackpot_win_type: int 遊戲內JP贏分類型
        @param jackpot_rtp_ver: int 遊戲內JP機率版本
        @param item_id: str 使用道具代碼
        @param buffer_bet: float 補分押分
        @param buffer_win: float 補分贏分
        @param item_win: float 道具卡贏分
        @param extra_bet: float 額外押注
        @param free_game_times: int FreeGame次數
        @param feature_game_type: str FeatureGame種類
        @param udid: str 裝置識別ID
        @param custom_data: str 自訂擴充
        @param session_id: str 登入流水號
        @return: bool 寫入成功或失敗
        """
        fun_name = 'bet_win'

        err, user_id = int_valid(fun_name, 'user_id', user_id, 0)
        if err is not None:
            self.logger.error(err)
            return False

        err, create_ts = timestamp_valid(fun_name, 'create_ts', create_ts)
        if err is not None:
            self.logger.info(err)

        if self.check_attributes:

            err, lv = int_valid(fun_name, 'lv', lv, 0)
            if err is not None:
                self.logger.error(err)
                return False

            err, vip_lv = int_valid(fun_name, 'vip_lv', vip_lv, 0)
            if err is not None:
                self.logger.error(err)
                return False

            err, curr_channel = str_valid(fun_name, 'curr_channel', curr_channel, '')
            if err is not None:
                self.logger.warn(err)

            err, rtp_ver = str_valid(fun_name, 'rtp_ver', rtp_ver, '')
            if err is not None:
                self.logger.warn(err)

            err, standard_total_bet = float_valid(fun_name, 'standard_total_bet', standard_total_bet, 0)
            if err is not None:
                self.logger.error(err)
                return False

            err, standard_total_win = float_valid(fun_name, 'standard_total_win', standard_total_win, 0)
            if err is not None:
                self.logger.error(err)
                return False

            err, game_type = str_valid(fun_name, 'game_type', game_type, '')
            if err is not None:
                self.logger.warn(err)

            err, room_id = str_valid(fun_name, 'room_id', room_id, '')
            if err is not None:
                self.logger.warn(err)

            err, game_id = str_valid(fun_name, 'game_id', game_id, '')
            if err is not None:
                self.logger.warn(err)

            err, fish_id = str_valid(fun_name, 'fish_id', fish_id, '')
            if err is not None:
                self.logger.warn(err)

            err, machine_type = str_valid(fun_name, 'machine_type', machine_type, '')
            if err is not None:
                self.logger.warn(err)

            err, coin_ratio = float_valid(fun_name, 'coin_ratio', coin_ratio, 0)
            if err is not None:
                self.logger.error(err)
                return False

            err, single_bet = float_valid(fun_name, 'single_bet', single_bet, 0)
            if err is not None:
                self.logger.error(err)
                return False

            err, bet_lines = int_valid(fun_name, 'bet_lines', bet_lines, 0)
            if err is not None:
                self.logger.error(err)
                return False

            err, machine_id = str_valid(fun_name, 'machine_id', machine_id, '')
            if err is not None:
                self.logger.warn(err)

            err, seat_num = str_valid(fun_name, 'seat_num', seat_num, '0')
            if err is not None:
                self.logger.warn(err)

            err, bet_coin_id = str_valid(fun_name, 'bet_coin_id', bet_coin_id, '0')
            if err is not None:
                self.logger.warn(err)

            err, bet_coin_balance_after = float_valid(fun_name, 'bet_coin_balance_after', bet_coin_balance_after, 0)
            if err is not None:
                self.logger.error(err)
                return False

            err, win_coin_id = str_valid(fun_name, 'win_coin_id', win_coin_id, '0')
            if err is not None:
                self.logger.warn(err)

            err, win_coin_balance_after = float_valid(fun_name, 'win_coin_balance_after', win_coin_balance_after, 0)
            if err is not None:
                self.logger.error(err)
                return False

            err, total_bet = float_valid(fun_name, 'total_bet', total_bet, 0)
            if err is not None:
                self.logger.error(err)
                return False

            err, total_win = float_valid(fun_name, 'total_win', total_win, 0)
            if err is not None:
                self.logger.error(err)
                return False

            err, net_bet = float_valid(fun_name, 'net_bet', net_bet, 0)
            if err is not None:
                self.logger.error(err)
                return False

            err, net_win = float_valid(fun_name, 'net_win', net_win, 0)
            if err is not None:
                self.logger.error(err)
                return False

            err, max_qualify_jp_type = int_valid(fun_name, 'max_qualify_jp_type', max_qualify_jp_type, 0)
            if err is not None:
                self.logger.error(err)
                return False

            err, jackpot_id = str_valid(fun_name, 'jackpot_id', jackpot_id, '')
            if err is not None:
                self.logger.warn(err)

            err, jackpot_type = str_valid(fun_name, 'jackpot_type', jackpot_type, '')
            if err is not None:
                self.logger.warn(err)

            err, jackpot_win = float_valid(fun_name, 'jackpot_win', jackpot_win, 0)
            if err is not None:
                self.logger.error(err)
                return False

            err, jackpot_win_type = int_valid(fun_name, 'jackpot_win_type', jackpot_win_type, 0)
            if err is not None:
                self.logger.error(err)
                return False

            err, jackpot_rtp_ver = int_valid(fun_name, 'jackpot_rtp_ver', jackpot_rtp_ver, 0)
            if err is not None:
                self.logger.error(err)
                return False

            err, item_id = str_valid(fun_name, 'item_id', item_id, '')
            if err is not None:
                self.logger.warn(err)

            err, buffer_bet = float_valid(fun_name, 'buffer_bet', buffer_bet, 0)
            if err is not None:
                self.logger.error(err)
                return False

            err, buffer_win = float_valid(fun_name, 'buffer_win', buffer_win, 0)
            if err is not None:
                self.logger.error(err)
                return False

            err, item_win = float_valid(fun_name, 'item_win', item_win, 0)
            if err is not None:
                self.logger.error(err)
                return False

            err, extra_bet = float_valid(fun_name, 'extra_bet', extra_bet, 0)
            if err is not None:
                self.logger.error(err)
                return False

            err, free_game_times = int_valid(fun_name, 'free_game_times', free_game_times, 0)
            if err is not None:
                self.logger.error(err)
                return False

            err, feature_game_type = str_valid(fun_name, 'feature_game_type', feature_game_type, '')
            if err is not None:
                self.logger.warn(err)

            err, udid = str_valid(fun_name, 'udid', udid, '')
            if err is not None:
                self.logger.warn(err)

            err, custom_data = str_valid(fun_name, 'custom_data', custom_data, '')
            if err is not None:
                self.logger.warn(err)

            err, session_id = str_valid(fun_name, 'session_id', session_id, '')
            if err is not None:
                self.logger.warn(err)

        else:
            lv = int_and_float_valid_base(lv, 0)
            vip_lv = int_and_float_valid_base(vip_lv, 0)
            curr_channel = str_valid_base(curr_channel, '')
            rtp_ver = str_valid_base(rtp_ver, '')
            standard_total_bet = int_and_float_valid_base(standard_total_bet, 0)
            standard_total_win = int_and_float_valid_base(standard_total_win, 0)
            game_type = str_valid_base(game_type, '')
            room_id = str_valid_base(room_id, '')
            game_id = str_valid_base(game_id, '')
            fish_id = str_valid_base(fish_id, '')
            machine_type = str_valid_base(machine_type, '')
            coin_ratio = int_and_float_valid_base(coin_ratio, 0)
            single_bet = int_and_float_valid_base(single_bet, 0)
            bet_lines = int_and_float_valid_base(bet_lines, 0)
            machine_id = str_valid_base(machine_id, '')
            seat_num = str_valid_base(seat_num, '0')
            bet_coin_id = str_valid_base(bet_coin_id, '0')
            bet_coin_balance_after = int_and_float_valid_base(bet_coin_balance_after, 0)
            win_coin_id = str_valid_base(win_coin_id, '0')
            win_coin_balance_after = int_and_float_valid_base(win_coin_balance_after, 0)
            total_bet = int_and_float_valid_base(total_bet, 0)
            total_win = int_and_float_valid_base(total_win, 0)
            net_bet = int_and_float_valid_base(net_bet, 0)
            net_win = int_and_float_valid_base(net_win, 0)
            max_qualify_jp_type = int_and_float_valid_base(max_qualify_jp_type, 0)
            jackpot_id = str_valid_base(jackpot_id, '')
            jackpot_type = str_valid_base(jackpot_type, '')
            jackpot_win = int_and_float_valid_base(jackpot_win, 0)
            jackpot_win_type = int_and_float_valid_base(jackpot_win_type, 0)
            jackpot_rtp_ver = int_and_float_valid_base(jackpot_rtp_ver, 0)
            item_id = str_valid_base(item_id, '')
            buffer_bet = int_and_float_valid_base(buffer_bet, 0)
            buffer_win = int_and_float_valid_base(buffer_win, 0)
            item_win = int_and_float_valid_base(item_win, 0)
            extra_bet = int_and_float_valid_base(extra_bet, 0)
            free_game_times = int_and_float_valid_base(free_game_times, 0)
            feature_game_type = str_valid_base(feature_game_type, '')
            udid = str_valid_base(udid, '')
            custom_data = str_valid_base(custom_data, '')
            session_id = str_valid_base(session_id, '')
        dt = self.get_logger_datetime(create_ts)
        dbw = {
            'ProDate': self.get_dt_pro_date(dt),
            'CreateTs': create_ts,
            'UserID': user_id,
            'LV': lv,
            'VipLV': vip_lv,
            'CurrChannel': curr_channel,
            'RTPVer': rtp_ver,
            'Standard_TotalBet': convert_large_numbers(standard_total_bet, self.large_numbers),
            'Standard_TotalWin': convert_large_numbers(standard_total_win, self.large_numbers),
            'GameType': game_type,
            'RoomID': room_id,
            'GameID': game_id,
            'FishID': fish_id,
            'MachineType': machine_type,
            'CoinRatio': convert_large_numbers(coin_ratio, self.large_numbers),
            'SingleBet': convert_large_numbers(single_bet, self.large_numbers),
            'BetLines': bet_lines,
            'MachineID': machine_id,
            'SeatNum': seat_num,
            'BetCoinID': bet_coin_id,
            'BetCoinBalanceAfter': convert_large_numbers(bet_coin_balance_after, self.large_numbers),
            'WinCoinID': win_coin_id,
            'WinCoinBalanceAfter': convert_large_numbers(win_coin_balance_after, self.large_numbers),
            'TotalBet': convert_large_numbers(total_bet, self.large_numbers),
            'TotalWin': convert_large_numbers(total_win, self.large_numbers),
            'NetBet': convert_large_numbers(net_bet, self.large_numbers),
            'NetWin': convert_large_numbers(net_win, self.large_numbers),
            'MaxQualifyJPType': max_qualify_jp_type,
            'JackpotID': jackpot_id,
            'JackpotType': jackpot_type,
            'JackpotWin': convert_large_numbers(jackpot_win, self.large_numbers),
            'JackpotWinType': jackpot_win_type,
            'JackpotRTPVer': jackpot_rtp_ver,
            'ItemID': item_id,
            'BufferBet': convert_large_numbers(buffer_bet, self.large_numbers),
            'BufferWin': convert_large_numbers(buffer_win, self.large_numbers),
            'ItemWin': convert_large_numbers(item_win, self.large_numbers),
            'ExtraBet': convert_large_numbers(extra_bet, self.large_numbers),
            'FreeGameTimes': free_game_times,
            'FeatureGameType': feature_game_type,
            'UDID': udid,
            'CustomData': custom_data,
            'SessionID': session_id,
        }
        self.ark_data_manager.send("DetailBetWin", dbw, create_ts, callback=self.callback,
                                   mongo_coll_date=self.get_dt_mongo_coll_date(dt))
        return True

    # 15.觸發自定義事件
    def send_custom_event(self, log_name, log_data, timestamp=None, check_func=None, callback=None):
        """

        @param callback: callback函式
        @param log_data: 自定義Log內容
        @param log_name: 自定義Log名稱
        @param timestamp: 資料發生時間,time.time(), 允許None, 若None系統會用現在時間
        @param check_func: 檢查驗證函式
        """
        fun_name = 'send_custom_event'

        if self.cdp_trans.get(log_name) is None:
            raise custom_event_not_exist

        if check_func is not None and check_func(log_data) is False:
            self.logger.warn('%s: %s  %s return False %s' % (fun_name, timestamp, log_name, log_data))
            return False

        # timestamp > 現在時間
        create_ts_err, create_ts_result = timestamp_valid(fun_name, 'timestamp', timestamp, allow_none=True)
        if create_ts_err is not None:
            self.logger.warn(create_ts_err)

        cb = callback if callback is not None else self.callback
        dt = self.get_logger_datetime(create_ts_result)
        log_data['ProDate'] = self.get_dt_pro_date(dt)
        self.ark_data_manager.send(log_name, log_data, create_ts_result, callback=cb, topic_prefix='Custom',
                                   mongo_coll_date=self.get_dt_mongo_coll_date(dt))
        return True
