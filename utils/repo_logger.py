import datetime
import logging
import os

class SingletonType(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonType, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class RepoLogger(object, metaclass=SingletonType):

    def __init__(self):
        self._logger = logging.getLogger()
        formatter = logging.Formatter('%(asctime)s \t [%(levelname)s | %(filename)s:%(lineno)s] > %(message)s')
        now = datetime.datetime.now()
        dirname = "./log"
        error_dir_name = dirname + '/error'
        debug_dir_name = dirname + '/debug'

        if not os.path.isdir(dirname):
            os.mkdir(dirname)
            
        if not os.path.isdir(error_dir_name):
            os.mkdir(error_dir_name)
            
        if not os.path.isdir(debug_dir_name):
            os.mkdir(debug_dir_name)

        stream_logger = logging.StreamHandler()
        stream_logger.setLevel(logging.ERROR)
        stream_logger.setFormatter(formatter)
        self._logger.addHandler(stream_logger)

        error_file_logger = logging.FileHandler(error_dir_name + "/error_log_" + now.strftime("%Y-%m-%d")+".log")
        error_file_logger.setLevel(logging.ERROR)
        error_file_logger.setFormatter(formatter)
        self._logger.addHandler(error_file_logger)

        debug_file_logger = logging.FileHandler(debug_dir_name + "/debug_log_" + now.strftime("%Y-%m-%d")+".log")
        debug_file_logger.setLevel(logging.DEBUG)
        debug_file_logger.setFormatter(formatter)
        self._logger.addHandler(debug_file_logger)
        self._logger.setLevel(logging.DEBUG)

    def get_logger(self):
        return self._logger
