try:
    from utils.repo_logger import RepoLogger
    logger = RepoLogger.__call__().get_logger()
except Exception as e:
    print("Error in log infrastructure!")

try: 
    logger.info("Importing Spark and Mysql packages...")
    import mysql.connector 
    from pyspark.sql import SparkSession
    from pyspark.sql.streaming import *
    from pyspark.sql.functions import split, col, when
    import pyspark.sql.functions as F
    logger.info("Successfully imported Spark and Mysql packages!")
except:
    logger.exception("An error has been occured while importing libraries.")

try:
    logger.info("Importing Configuration Variables...")
    from utils.tools import MysqlConnection, KafkaConnection, KafkaTopics, SampleDataPaths
    MYSQL_USER = MysqlConnection.__call__().get_mysql_user()
    MYSQL_DATABASE = MysqlConnection.__call__().get_mysql_database()
    MYSQL_TABLE = MysqlConnection.__call__().get_mysql_table_case1()
    MYSQL_PASSWORD = MysqlConnection.__call__().get_mysql_pwd()
    MYSQL_HOST = MysqlConnection.__call__().get_mysql_host()
    MYSQL_PORT = MysqlConnection.__call__().get_mysql_port()
    TRANSACTION_TOPIC = KafkaTopics.__call__().get_transaction_topic()
    BOOTSTRAP_SERVERS = KafkaConnection.__call__().get_bootstrap_servers()        
    CUSTOMER_DATA_PATH = SampleDataPaths.__call__().get_customer_csv_path()
    EXCHANGE_RATE_DATA_PATH = SampleDataPaths.__call__().get_exchange_rate_csv_path()
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
        transaction_id   = data_dict["transaction_id"]
        customer_id = data_dict["customer_id"]
        transaction_date = data_dict["transaction_date"]
        country = data_dict["country"]
        eur_amount = data_dict["eur_amount"]

        transaction_val=(transaction_id, customer_id, transaction_date, country, eur_amount)
        mycursor.execute("USE {};".format(MYSQL_DATABASE))
        insert_command = 'INSERT INTO ' + MYSQL_TABLE + ' (transaction_id, \
                        customer_id, transaction_date, country, eur_amount) ' \
                        + 'VALUES' + ' ( %s, %s, %s, %s, %s)'         
        mycursor.execute(insert_command, transaction_val)
        mydatabase.commit()
        logger.info("Successfully wrote row to MySQL in write_to_mysql function.")
    except:
        logger.exception("An error occured while writing to MySQL in write_to_mysql function.")

class SparkPipeline():
    """ This class consumes stream transaction messages from Kafka and extracts batch csv's to Spark Streaming. 
        Then SparkPipeline class transforms all data and join them to write MySQL table.
    """
    def __init__(self):
        """ This constructor builds SparkSession."""
        try:
            self.spark = SparkSession \
                            .builder  \
                            .appName("Friendsurance Case One") \
                            .master("local[1]")  \
                            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1")  \
                            .getOrCreate()
            logger.info("Successfully created object of SparkPipeline class.")
        except:
            logger.exception("An error has been occured while creating constructor of SparkPipeline class.")

    def load_batch_data(self):
        """ This function extracts and transforms Customer.csv and Exchange_rate.csv."""
        try:
            self.customer_df=self.spark.read.format("csv").option("header","true")\
                                            .option("delimiter",",").load(CUSTOMER_DATA_PATH) 
            self.exchange_rate_df=self.spark.read.format("csv").option("header","true")\
                                            .option("delimiter",",").load(EXCHANGE_RATE_DATA_PATH) 
            
            self.customer_df=self.customer_df.dropDuplicates(subset=["customer_id"]) 
            self.customer_df=self.customer_df.drop("customer_age","customer_gender") # we dont use age and gender

            self.exchange_rate_df=self.exchange_rate_df.dropDuplicates()
            self.exchange_rate_df=self.exchange_rate_df.filter(self.exchange_rate_df.rate != "1") # we dont use rate with 1
            self.exchange_rate_df.show()
            logger.info("Successfully completed load_batch_data function.")
        except:
            logger.exception("An error occured in load_batch_data function.")

    def run_stream(self):
        """ This function starts a stream pipeline which consumes transaction messages from Kafka."""
        try:
            self.transaction_df = self.spark.readStream.format("kafka") \
                                        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
                                        .option("subscribe", TRANSACTION_TOPIC) \
                                        .option("includeHeaders", "true") \
                                        .option("startingOffsets", "latest") \
                                        .load()
            logger.info("Successfully completed run_stream function!")
        except:
            logger.exception("An error occured while initializing a stream pipeline in run_stream function.")

    def transform_stream_data(self):
        """ This function transforms stream data."""
        try:
            self.transaction_df=self.transaction_df.selectExpr("CAST(value AS STRING)")
            self.transaction_df=self.transaction_df.withColumn("NEW",F.expr("substring(value, 2, length(value)-4)"))
            split_col = split(self.transaction_df["NEW"], ',')
            self.transaction_df = self.transaction_df \
                                    .withColumn('transaction_id', split_col.getItem(0)) \
                                    .withColumn('customer_id', split_col.getItem(1)) \
                                    .withColumn('transaction_type', split_col.getItem(2)) \
                                    .withColumn('amount', split_col.getItem(3).cast("float")) \
                                    .withColumn('currency', split_col.getItem(4)) \
                                    .withColumn('transaction_date', split_col.getItem(5)) # convert it to date format later.
            self.transaction_df=self.transaction_df.drop("NEW","value")
            logger.info("Successfully completed transform_stream_data function!")
        except:
            logger.exception("An error occured while initializing transform_stream_data function.")

    def join_data(self):
        """ This function joins 3 dataframes: Customer, Exchange_rate and Transaction."""
        try:
            self.customer_with_transaction = self.transaction_df.join(self.customer_df, 
                                                                    self.customer_df.customer_id == self.transaction_df.customer_id)

            self.df_all=self.customer_with_transaction.join(self.exchange_rate_df, 
                                                            ((self.exchange_rate_df.effective_date == self.customer_with_transaction.transaction_date) 
                                                            & (self.exchange_rate_df.to_currency == self.customer_with_transaction.currency)) ,
                                                            how="left")           

            self.df_all=self.df_all.withColumn("gbp_amount",(when((col("currency")== "GBP"), self.df_all.amount).otherwise(self.df_all.amount/self.df_all.rate)))
            self.df_all=self.df_all.drop("exchange_rate_id","effective_date","transaction_type","from_currency","to_currency","rate")
            
            self.df_all=self.df_all.join(self.exchange_rate_df, 
                                        ((self.df_all.transaction_date==self.exchange_rate_df.effective_date) &
                                        (self.exchange_rate_df.to_currency=="EUR")))
                                    
            self.df_all=self.df_all.withColumn("eur_amount", (when((col("currency")== "EUR"), self.df_all.amount).otherwise(self.df_all.gbp_amount*self.df_all.rate)))
            self.df_all=self.df_all.drop("from_currency","to_currency","exchange_rate_id","effective_date","rate","amount","currency","gbp_amount")
            logger.info("Successfully completed join_data function!")
        except:
            logger.exception("An error occured while initializing join_data function.")

    def complete(self): 
        """ This function runs Spark Streaming and sends transformed data to write_my_sql function."""
        try:
            self.df_all.writeStream.format("append").foreach(write_to_mysql).start().awaitTermination()   
        except:
            logger.exception("An error occured while initializing complete function.")

if __name__ == "__main__":
    object = SparkPipeline()
    object.load_batch_data()
    object.run_stream()
    object.transform_stream_data()
    object.join_data()
    object.complete()
    