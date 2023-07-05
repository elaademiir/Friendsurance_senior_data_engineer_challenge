import sys
import yaml

try:
    from utils.repo_logger import RepoLogger
    logger = RepoLogger.__call__().get_logger()
except Exception as e:
    print("Error in log infrastructure in ml pipeline tools!")

# SingletonType Class to be inherited in other Classes in Tools.
class SingletonType(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonType, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

# Importing the Config File
try:
    path = sys.path
    project_root = "/Users/eodemir/Documents/GitHub/Friendsurance_senior_data_engineer_challenge"
    assert project_root != None
    config_file = project_root + "/config.yml"
    print('CONFIG:', config_file)
    with open(config_file, 'r') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
except Exception as e:
    logger.exception(e)
finally:
    logger.info(".")
    logger.info("..")
    logger.info("Repo is starting...")
    logger.info("Configuration file is read successfully.")

class KafkaConnection(object, metaclass=SingletonType):
    """
    Setting the Kafka configuration to connect.
    """
    def __init__(self) -> None:
        self._bootstrap_servers_local = config["kafka_connection"]["bootstrap_servers"]
        self._scan_startup_mode = config["kafka_connection"]["scan_startup_mode"]
        self._value_format = config["kafka_connection"]["value_format"]
        self._sync_time_second = config["kafka_connection"]["sync_time_second"]
        logger.info("Adjusted KafkaConnection in order to move the data.")

    def get_bootstrap_servers(self):
        return self._bootstrap_servers_local

    def get_scan_startup_mode(self):
        return self._scan_startup_mode

    def get_value_format(self):
        return self._value_format

    def get_sync_time_second(self):
        return self._sync_time_second

class KafkaTopics(object, metaclass=SingletonType):
    """
    Setting the Kafka topics.
    """
    def __init__(self) -> None:
        self._customer_topic = config['kafka_topics']['customer_topic']
        self._exchange_rate_topic = config['kafka_topics']['exchange_rate_topic']
        self._transaction_topic = config['kafka_topics']['transaction_topic']

    def get_customer_topic(self):
        return self._customer_topic

    def get_exchange_rate_topic(self):
        return self._exchange_rate_topic

    def get_transaction_topic(self):
        return self._transaction_topic
    
class SampleDataPaths(object, metaclass=SingletonType):
    """
    Setting Sample Data Paths.
    """
    def __init__(self) -> None:
        self._customer_csv_path = config['sample_data_paths']['customer_csv']
        self._exchange_rate_csv_path= config['sample_data_paths']['exchange_rate_cv']
        self._transaction_csv_path = config['sample_data_paths']['transaction_csv']

    def get_customer_csv_path(self):
        return self._customer_csv_path

    def get_exchange_rate_csv_path(self):
        return self._exchange_rate_csv_path

    def get_transaction_csv_path(self):
        return self._transaction_csv_path

class MysqlConnection(object, metaclass=SingletonType):
    """
    Setting the MySQL configuration to connect.
    """
    def __init__(self) -> None:
        self._mysql_user = config["mysql_connection"]["user"]
        self._mysql_pwd = config["mysql_connection"]["pwd"]
        self._mysql_database = config["mysql_connection"]["database"]
        self._mysql_table_case1 = config["mysql_connection"]["table_case1"]
        self._mysql_table_case2 = config["mysql_connection"]["table_case2"]
        self._mysql_host = config["mysql_connection"]["host"]
        self._mysql_port = config["mysql_connection"]["port"]
        logger.info("Adjusted MySQL Connection in order to load the data.")

    def get_mysql_user(self):
        return self._mysql_user

    def get_mysql_pwd(self):
        return self._mysql_pwd

    def get_mysql_database(self):
        return self._mysql_database

    def get_mysql_table_case1(self):
        return self._mysql_table_case1

    def get_mysql_table_case2(self):
        return self._mysql_table_case2

    def get_mysql_host(self):
        return self._mysql_host

    def get_mysql_port(self):
        return self._mysql_port


