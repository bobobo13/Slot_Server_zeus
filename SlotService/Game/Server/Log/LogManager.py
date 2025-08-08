# -*- coding: utf-8 -*-
import logging
import logging.handlers
import sys
import platform
import os

try:
    import distro
except ImportError:
    distro = None

try:
    from concurrent_log_handler import ConcurrentRotatingFileHandler
except ImportError:
    raise ImportError("缺少必要套件 'concurrent-log-handler'，請先執行：pip install concurrent-log-handler")

class LogManager:
    FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
    NOCONSOLE = False
    loggerMap = {}  # 改成 class attribute，避免 global 汙染

    @classmethod
    def setup_linux_handler(cls, logger, code_name, logPath, maxBytes, backupCount, loglevel):
        try:
            distro_name = distro.id().lower() if distro else ""

            if distro_name in ['rhel', 'centos', 'ubuntu', 'debian']:
                # 日誌文件路徑
                filename = os.path.join(logPath or os.path.join(os.environ.get('HOME', '/tmp'), 'log'), f'{code_name}.log')
                os.makedirs(os.path.dirname(filename), exist_ok=True)

                handler = ConcurrentRotatingFileHandler(
                    filename, "a", maxBytes=maxBytes, backupCount=backupCount
                )
                handler.setLevel(loglevel)
                formatter = logging.Formatter(cls.FORMAT)
                handler.setFormatter(formatter)

                logger.addHandler(handler)

                logger.info(f"Linux file logger initialized: {filename}")

        except Exception as e:
            logger.warning(f"⚠Failed to setup Linux log handler: {e}")

    def __init__(self, code_name='Engine', filename='', logPath='', maxBytes=104857600, backupCount=10, loglevel='INFO'):
        if code_name in self.loggerMap:
            raise Exception(f'{code_name} is already used in SysLog')

        self.code_name = code_name
        self.loglevel = self.get_log_level(loglevel)

        # 確保基礎配置乾淨
        logging.basicConfig(level=self.loglevel, format=self.FORMAT, force=True)

        self.logger = logging.getLogger(self.code_name)
        self.logger.setLevel(self.loglevel)

        self.change_format(self.logger)

        # OS 適配
        if platform.system().lower() == "linux":
            self.setup_linux_handler(self.logger, code_name, logPath, maxBytes, backupCount, self.loglevel)

        self.logger.info(f"Logger initialized for: {self.code_name}")
        self.loggerMap[self.code_name] = self.logger

    @staticmethod
    def sys_log():
        logger = logging.getLogger('Engine')
        LogManager.change_format(logger)
        return logger

    def getLogger(self, name):
        if name in self.loggerMap:
            return self.loggerMap[name]

        logger = logging.getLogger(name)
        logger.setLevel(self.loglevel)
        self.change_format(logger)
        self.loggerMap[name] = logger

        logger.info(f"Sub-logger initialized: {name}")
        return logger

    @staticmethod
    def get_log_level(loglevel):
        level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        return level_map.get(loglevel.upper(), logging.DEBUG)

    @classmethod
    def change_format(cls, logger):
        # 清理舊 handlers，避免重複
        logger.handlers = [h for h in logger.handlers if not isinstance(h, logging.StreamHandler)]

        if cls.NOCONSOLE:
            return

        formatter = logging.Formatter(cls.FORMAT)

        # stdout handler (INFO 以下)
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.DEBUG)
        stdout_handler.addFilter(lambda record: record.levelno < logging.ERROR)
        stdout_handler.setFormatter(formatter)

        # stderr handler (ERROR 以上)
        stderr_handler = logging.StreamHandler(sys.stderr)
        stderr_handler.setLevel(logging.ERROR)
        stderr_handler.setFormatter(formatter)

        logger.addHandler(stdout_handler)
        logger.addHandler(stderr_handler)

