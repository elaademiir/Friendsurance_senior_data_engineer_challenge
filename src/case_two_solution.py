try:
    from utils.repo_logger import RepoLogger
    logger = RepoLogger.__call__().get_logger()
except Exception as e:
    print("Error in log infrastructure!")

try: 
    logger.info("Importing Spark and Mysql packages...")
    from pyspark.sql import SparkSession
    from pyspark.sql.streaming import *
    import pyspark.sql.functions as F
    import mysql.connector 
    logger.info("Successfully imported Spark and Mysql packages!")
except:
    logger.info("An error has been occured while importing libraries.")

try:
    logger.info("Importing Configuration Variables...")
    from utils.tools import MysqlConnection, KafkaConnection, KafkaTopics
    MYSQL_USER = MysqlConnection.__call__().get_mysql_user()
    MYSQL_DATABASE = MysqlConnection.__call__().get_mysql_database()
    MYSQL_TABLE = MysqlConnection.__call__().get_mysql_table_case2()
    MYSQL_PASSWORD = MysqlConnection.__call__().get_mysql_pwd()
    MYSQL_HOST = MysqlConnection.__call__().get_mysql_host()
    MYSQL_PORT = MysqlConnection.__call__().get_mysql_port()
    EXCHANGE_RATE_TOPIC = KafkaTopics.__call__().get_exchange_rate_topic()
    BOOTSTRAP_SERVERS = KafkaConnection.__call__().get_bootstrap_servers()        
    logger.info("Successfully imported Configuration Variables!")
except:
    logger.exception("An error occured while importing configuration variables.")

def write_to_mysql(row):
    """ This function converts and writes transformed data to Mysql table."""
    try:
        logger.info("Starting to connect MySQL...")
        mydatabase = mysql.connector.connect(user=MYSQL_USER, 
                                            database=MYSQL_DATABASE,
                                            password=MYSQL_PASSWORD,
                                            host=MYSQL_HOST,
                                            port=MYSQL_PORT)
        mycursor= mydatabase.cursor()
        logger.info("Successfully connected to MySQL in write_to_mysql function!")
    except:
        logger.exception("An error occured while connecting MySQL in write_to_mysql function.")
    
    try:
        logger.info("Starting to write Spark streaming row to MySQL...")
        data_dict = row.asDict()
        to_currency = data_dict["to_currency"]
        effective_date = data_dict["effective_date"]
        rate = data_dict["rate"]

        exchange_rate_val=(to_currency, effective_date, rate)
        mycursor.execute("USE {};".format(MYSQL_DATABASE))
        insert_command = 'INSERT INTO ' + MYSQL_TABLE + ' (to_currency, \
                        effective_date, rate) ' \
                        + 'VALUES' + ' (%s, %s, %s)'         
        mycursor.execute(insert_command, exchange_rate_val)
        mydatabase.commit()
        logger.info("Successfully wrote row to MySQL in write_to_mysql function.")
    except:
        logger.exception("An error occured while writing to MySQL in write_to_mysql function.")

class SparkPipeline():
    """ This class consumes stream exchange_rate messages from Kafka.
        Then SparkPipeline class transforms all data and send them to write MySQL table.
    """
    def __init__(self):
        """ This constructor builds SparkSession."""
        try:
            self.spark = SparkSession \
                .builder \
                .appName("Friendsurance Case Two") \
                .master("local[1]") \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1") \
                .getOrCreate()
            logger.info("Successfully created object SparkPipeline.")
        except:
            logger.exception("An error has been occured while creating constructor of SparkPipeline class.")

    def stream_data_load(self):
        """ This function starts a stream pipeline which consumes exchange_rate messages from Kafka."""
        try:
            self.exchange_rate_df = self.spark \
                                    .readStream \
                                    .format("kafka") \
                                    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
                                    .option("subscribe", EXCHANGE_RATE_TOPIC) \
                                    .option("includeHeaders", "true") \
                                    .option("startingOffsets", "latest") \
                                    .load()
            logger.info("Successfully completed stream_data_load function!")
        except:
            logger.exception("An error occured while initializing stream_data_load function.")

    def data_transform(self):
        """ This function transforms stream data."""
        try:
            self.exchange_rate_df=self.exchange_rate_df.selectExpr("CAST(value AS STRING)")
            self.exchange_rate_df=self.exchange_rate_df.withColumn("NEW",F.expr("substring(value, 2, length(value)-4)"))
            
            split_col = F.split(self.exchange_rate_df["NEW"], ',')
            self.exchange_rate_df = self.exchange_rate_df \
                                    .withColumn('transaction_id', split_col.getItem(0)) \
                                    .withColumn('to_currency', split_col.getItem(2)) \
                                    .withColumn('effective_date', split_col.getItem(3)) \
                                    .withColumn('rate', split_col.getItem(4)) 

            self.exchange_rate_df=self.exchange_rate_df.drop("NEW","value","transaction_id") \
                                                        .filter(self.exchange_rate_df.rate != "1") #from 6304 to 4728 rows!
            logger.info("Successfully completed data_transform function!")
        except:
            logger.exception("An error occured while initializing data_transform function.")

    def data_insert(self): 
        """ This function runs Spark Streaming and sends transformed data to write_my_sql function."""
        try:
            query=self.exchange_rate_df.writeStream.format("append").option("checkpointLocation", 
                                                    "./checkpointlocation").foreach(write_to_mysql).start()
            query.awaitTermination()
        except:
            logger.exception("An error occured while initializing data_insert function.")

if __name__ == "__main__":
    object = SparkPipeline()
    object.stream_data_load()
    object.data_transform()
    object.data_insert()
 
    