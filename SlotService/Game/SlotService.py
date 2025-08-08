
# try:
#     from .ArkGame.GameServer import GameServer
# except ImportError:
#     from .Server.GameServer import GameServer

from .ArkEngine.ArkGame.GameServer import GameServer
import sys, os

from .Server.Database.MongoFactory import MongoFactory
from .Module.LogDao import LogDao
from .Module import Util, UniqueSnCache
from .Module.Dimension import Dimension


from .Module.pixiu.user_manager import UserManager
from .Module.pixiu.IgStage import IgStage
from .Module.pixiu.MacrossPlatformAgent import MacrossPlatformAgent
from .Module.pixiu.ApiWallet import ApiWallet
from .Module.pixiu.currency_manager import CurrencyManager
from .SlotSystem import SlotSystem
from .Wallet.WalletManager import WalletManager
from .Wallet.MongoWallet import MongoWallet

from .Buffer.Buffer import Buffer
from .SlotBonus.SlotBonus import SlotBonus
from .ArkCDP.data_model.DataEvent import DataEvent
from .Slot.SlotManager import SlotManager

class SlotService(GameServer):
    def __init__(self, code_name, global_code_name, env, Role, Channel=None):
        self.env = env
        self.Role = Role
        self.is_test = (lambda env: env in {"local", "dev", "test"})(env)
        version = env if Channel is None else (env + '-' + Channel)
        super(SlotService, self).__init__(code_name, version, global_code_name)

        self.MongoFactory = MongoFactory(version, host_cfg_path=self.get_config_path() + "mongo_config.cfg", db_cfg_path=self.get_config_path() + "mongo_factory.cfg")
        self.LogDao = LogDao.get_instance(self, self.getLogger(), BackendLogDataSrc=self.MongoFactory.get_database("BackendLog"), CommonLogDataSrc=self.MongoFactory.get_database("BackendLog"))
        self.UserManager = UserManager(logger=self.getLogger(), UserInfoDb=self.MongoFactory.get_database('UserInfo'))
        self.GetPlayerDataFunc = self.UserManager.get_user_data

        self._CurrencyManager = CurrencyManager(Logger=self.getLogger(), DataSource=self.MongoFactory.get_database("MainGame"))

        # ArkCDP
        send_custom_event_func = None
        if not self.is_test:
            cdp_cfg_path = self.get_config_path() + "cdp.cfg"
            cdp_trans_path = self.get_config_path() + "cdp_trans.json"
            cdp_trans_custom_path = self.get_config_path() + "cdp_trans_custom.json"
            self.arkCDP = DataEvent(cdp_cfg_path, self.logger.sys_log(), "macross", env, cdp_trans_path=cdp_trans_path, cdp_trans_custom_path=cdp_trans_custom_path)
            send_custom_event_func = self.arkCDP.send_custom_event

        self._ApiServer = None
        if self.env not in {"local"}:
            self._ApiServer = MacrossPlatformAgent(logger=self.getLogger(), config_path=self.get_config_path()+"ark_game.cfg", CommonLog=self.LogDao.CommonLog, GetPlayerDataFunc=self.GetPlayerDataFunc, CurrencyManager=self._CurrencyManager)
        self.getLogger().info(f"ApiServer: {self._ApiServer or 'local'}")
        main_wallet, second_wallet, second_wallet_from_type = self.get_wallet()

        # WalletManager
        kwargs = {
            'GetPlayerDataFunc': self.GetPlayerDataFunc,
            'CommonLogFunc': self.LogDao.CommonLog,
            'AddSessionLogFunc': self.LogDao.addSessionGameLog,
            'ErrorEventUrl': Util.GetConfigOption(self.get_config_path()+"ark_game.cfg", "ErrorEvent", "ErrorEventUrl", default=None),
            'SecondWallet': second_wallet,
            "SecondWalletFromType": second_wallet_from_type,
        }
        self.walletMgr = WalletManager(main_wallet, self.getLogger(), **kwargs)

        self.GameSnCache = UniqueSnCache.UniqueSnCache("api_game_sn", self.MongoFactory.get_database("GameSN"))

        self.Stage = IgStage(DataSource=self.MongoFactory.get_wrap_database("Lobby"), Logger=self.getLogger())
        # self._PlatformAgent = PlatformAgent(self.getLogger(), UrlConfig=pixiu_config_path+"ark_game.cfg", ConfigSection="GamePlatform")

        # Dimension
        self.Dimension = Dimension(DataSource=self.MongoFactory.get_wrap_database("Dimension"), Logger=self.getLogger(), GetUserDataFunc=self.GetPlayerDataFunc)
        self.GetInfoFunc = self.Dimension.GetInfo
        self.InfoRegisterFunc = self.Dimension.InfoRegister


        # Buffer
        self.KioskBuffer = Buffer(self.getLogger(), self.MongoFactory.get_database("GameRate"), Name="Kiosk", GetPlayerDataFunc=self.GetPlayerDataFunc, FilePath="Script/Init/KioskBufferSetting.csv")


        self.CommonBuffer = Buffer(self.getLogger(), DataSource=self.MongoFactory.get_database("GameRate"), Name='Common', GetPlayerDataFunc=self.GetPlayerDataFunc, GetInfoFunc=self.GetInfoFunc, InfoRegisterFunc=self.InfoRegisterFunc, FilePath="Script/Init/CommonBufferSetting.csv")
        self.BuyBonusBuffer = Buffer(self.getLogger(), DataSource=self.MongoFactory.get_database("GameRate"), Name='BuyBonus', GetPlayerDataFunc=self.GetPlayerDataFunc, GetInfoFunc=self.GetInfoFunc, InfoRegisterFunc=self.InfoRegisterFunc, FilePath="Script/Init/BuyBonusBufferSetting.csv")
        self.BuyBonus = SlotBonus(self.getLogger(), DataSource=self.MongoFactory.get_database("BuyBonus"), Name='BuyBonus', ConfigPath=self.get_config_path(), GetInfoFunc=self.GetInfoFunc, InfoRegisterFunc=self.InfoRegisterFunc, GetPlayerDataFunc=self.GetPlayerDataFunc)

        # SlotManager
        kwargs = {
            "DataSource": self.MongoFactory.get_wrap_database("SlotGame"),
            "GameStateDataSource": self.MongoFactory.get_wrap_database("SlotGame")
        }

        self.slot_manager = SlotManager(self.getLogger(), **kwargs)
        # SlotService
        kwargs = {
            "SlotDataSource": self.MongoFactory.get_database("SlotGame"),
            "BuyBonusDataSource": self.MongoFactory.get_database("BuyBonus"),
            "GameRateDataSource": self.MongoFactory.get_database("GameRate"),
            "HistoryDataSource": self.MongoFactory.get_database("History"),
            "BackendLogDataSource": self.MongoFactory.get_database("BackendLog"),
            "WalletManager": self.walletMgr,
            'GetPlayerDataFunc': self.GetPlayerDataFunc,
            "GetGameDataFunc": self.UserManager.get_game_data,
            "GetGameSnFunc": self.GameSnCache.get_sn,
            "GetTokenFunc": self.getToken,
            "LobbyStage": self.Stage,
            "PixiuLobbyUrl": Util.GetConfigOption(self.get_config_path()+"ark_game.cfg", "GamePlatform", "Url", default=None),
            "BuyBonus": self.BuyBonus,
            "KioskBuffer": self.KioskBuffer,
            "CommonBuffer": self.CommonBuffer,
            "BuyBonusBuffer": self.BuyBonusBuffer,
            'send_custom_event_func': send_custom_event_func,  # ArkCDP
            "SlotManager": self.slot_manager,
        }
        self._SlotMiddlewareSys = SlotSystem(self, self.getLogger(), **kwargs)
        self.register("SlotGame", self._SlotMiddlewareSys)

    def get_wallet(self):
        kwargs = {
            # 'GetPlayerData': self.dimension.GetDimension,
            'GetPlayerData': self.GetPlayerDataFunc,
            'CommonLog': self.LogDao.CommonLog,
            'ApiServer': self._ApiServer,
            # 'KickPlayer': self.KickPlayer,
            # AW
            'WalletLockConfig': self.get_config_path() + "ark_game.cfg",
            # 'MerchantMgr': self.merchantInfoManager
        }
        apiWallet = ApiWallet(self.getLogger(), None, **kwargs) if self.env not in {"local"} else None
        mongoWallet = MongoWallet(self.getLogger(), self.MongoFactory.get_database("Asset"))
        mainWallet = apiWallet
        secondWallet = None
        SecondWalletFromType = []
        if self.env in {"local"}:
            mainWallet = mongoWallet
        elif self.env in {"dev", "test", "uat"}:
            secondWallet = mongoWallet
            SecondWalletFromType = ["webgl", "demo"]
        self.getLogger().info(f"mainWallet: {mainWallet}, secondWallet: {secondWallet}, SecondWalletFromType: {SecondWalletFromType}")
        return mainWallet, secondWallet, SecondWalletFromType

    def get_config_path(self, code_name=None):
        if code_name is None:
            code_name = self.codeName
        if sys.platform.startswith("linux"):
            return os.path.join('/etc', 'igs', code_name, 'Game', 'config', self.version) + '/'
        if self.version == "local" and self.Role == "Simple":
            return "./config/{}/".format(self.version)
        return "../../Config/{}/{}/".format(code_name, self.version)

    def ark_command(self, data, *args, **kwargs):
        ark_id = data['ark_id']
        result = super(SlotService, self).ark_command(data, *args, **kwargs)
        self.UserManager.clean_cached_user_data(ark_id)
        self.getLogger().debug(f"clean_cached_user_data: {ark_id}")
        return result


