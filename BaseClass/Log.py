import logging
from logging.handlers import TimedRotatingFileHandler

class Loger(object):
    def __init__(self, name='', level='debug', logfile=None, verbose=False):
        '''
        初始化Loger对象

        :param name: loger名称
        :param level: 记录级别
        :param logfile: 日志文件（为None时仅终端显示）
        :param verbose: 当日志文件被设置时，是否在终端显示记录信息
        '''
        if level == 'debug':
            self._level = logging.DEBUG
        elif level == 'info':
            self._level = logging.INFO
        elif level == 'warning':
            self._level = logging.WARNING
        elif level == 'error':
            self._level = logging.ERROR
        elif level == 'critical':
            self._level = logging.CRITICAL
        self._formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        if logfile:
            self._file_handler = TimedRotatingFileHandler(logfile, when='H', interval=4, backupCount=180, encoding='utf-8')
            self._file_handler.setLevel(self._level)
            self._file_handler.setFormatter(self._formatter)
        else:
            self._file_handler = None

        self._console_handler = logging.StreamHandler()
        self._console_handler.setLevel(self._level)
        self._console_handler.setFormatter(self._formatter)

        self._logger = logging.getLogger(name)
        self._logger.setLevel(self._level)
        self._logger.addHandler(self._console_handler)
        if self._file_handler:
            self._logger.addHandler(self._file_handler)
            if not verbose:
                self._logger.removeHandler(self._console_handler)

    def debug(self, msg):
        self._logger.debug(msg)

    def info(self, msg):
        self._logger.info(msg)

    def warning(self, msg):
        self._logger.warning(msg)

    def error(self, msg):
        self._logger.error(msg)

    def critical(self, msg):
        self._logger.critical(msg)
