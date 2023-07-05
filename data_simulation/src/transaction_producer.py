try:
    import time
    import json
    from kafka import KafkaProducer
except:
    print("An error has been occured while importing libraries.")

try:
    from utils.repo_logger import RepoLogger
    logger = RepoLogger.__call__().get_logger()
except Exception as e:
    print("An error has been occured in log infrastructure!")

try:
    from utils.tools import KafkaTopics, SampleDataPaths, KafkaConnection
    TRANSACTION_TOPIC = KafkaTopics.__call__().get_transaction_topic()
    TRANSACTION_DATA_PATH = SampleDataPaths.__call__().get_transaction_csv_path()
    BOOTSTRAP_SERVERS = KafkaConnection.__call__().get_bootstrap_servers()
    SCAN_STARTUP_MODE = KafkaConnection.__call__().get_scan_startup_mode()
    VALUE_FORMAT = KafkaConnection.__call__().get_value_format()
    SYNC_TIME_SECOND = KafkaConnection.__call__().get_sync_time_second()
except:
    logger.exception("An error has been occured while loading Kafka configuration values.")

class SimulationKafkaProducer():
    """ This class simulates generating Kafka Messages with data sources in csv format.
        There are 2 option to send Kafka Messages: async and sync.
    """

    def __init__(self):
        """ This constructor sets KafkaProducer API."""
        try:
            self.main_producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, 
                                                value_serializer=lambda v: json.dumps(v).encode("utf-8"))
            assert self.main_producer != None
            logger.info("Successfully initialized __init__ in SimulationKafkaProducer class!")
        except Exception as e:
            logger.exception("Error __init__: {}".format(e))

    def success(self, metadata):
        logger.info("Succesfully sent message to topic: {}".format(metadata.topic))

    def error(self, metadata, exception):
        logger.exception("Oops... Could not send message to topic: {} with partition: {} exception: {} "
                    .format(metadata.topic, metadata.partition, exception))

    def kafka_producer_sync(self, topic, msg):
        """ This function sends message with KafkaProducer API synchronously."""
        self.main_producer.send(topic, msg).add_callback(self.success).add_errback(self.error)
        self.main_producer.flush()
        time.sleep(SYNC_TIME_SECOND)

    def kafka_producer_async(self, topic, msg):
        """ This function sends message with KafkaProducer API asynchronously."""
        self.main_producer.send(topic, msg).add_callback(self.success).add_errback(self.error)
        self.main_producer.flush()

    def kafka_send_msg(self, topic, data_path):
        """ This function reads data source in csv format and runs KafkaProducer API."""
        with open (data_path, "r") as myfile:
            data_source = myfile.readlines()
            for row in data_source:
                self.kafka_producer_async(topic, msg=row)

if __name__=='__main__':
    obj=SimulationKafkaProducer()
    obj.kafka_send_msg(topic=TRANSACTION_TOPIC, data_path=TRANSACTION_DATA_PATH)
