from Encryption import EncryptionDriver
from Decryption import DecryptionDriver
#from uuid import UniqueIdDriver
from pyspark.sql import SparkSession
from ConfigProcessor import get_config_details
import os


class DataProcessingDriver:
    os.environ['HADOOP_HOME'] = 'C:\\hadoop-3.3.6'

    def create_spark_session(self):
        spark = SparkSession.builder.appName("YourSparkJob").getOrCreate()
        return spark


if __name__ == "__main__":
    # Step 1: initialization and object creation
    data_processing = DataProcessingDriver()

    # 3
    spark = data_processing.create_spark_session()

    # 2
    conf = get_config_details()

    # 3
    EncryptionDriver.encrypt_data(conf, spark)
    #encrypt_data(conf, spark)

    # 4
    DecryptionDriver.decrypt_and_read_data(conf, spark)

    #5
    #UniqueIdDriver.process_data_uuid(conf, spark)
