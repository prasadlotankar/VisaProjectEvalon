from Encryption import EncryptionDriver
from Decryption import DecryptionDriver
from Id import IdDriver
from qc import QualityCheck
from adls import AdlsUpload
from pyspark.sql import SparkSession
from ConfigProcessor import get_config_details
import os


# Define a class for data processing operations
class DataProcessingDriver:
    # Set HADOOP_HOME environment variable
    os.environ['HADOOP_HOME'] = 'C:\\hadoop-3.3.6'

    # Method to create a Spark session
    def create_spark_session(self):
        spark = SparkSession.builder.appName("YourSparkJob").getOrCreate()
        return spark


# Main entry point for the script
if __name__ == "__main__":
    # Step 1: Initialization and object creation
    data_processing = DataProcessingDriver()

    # Step 2: Get configuration details
    conf = get_config_details()

    # Step 3: Create a Spark session
    spark = data_processing.create_spark_session()

    # Step 4: Encryption
    EncryptionDriver.encrypt_data(conf, spark)

    # Step 5: Decryption
    DecryptionDriver.decrypt_and_read_data(conf)

    # Step 6: UUID Generation
    IdDriver.process_data_uuid(conf, spark)

    # Step 7: Quality Check
    QualityCheck.perform_qc(conf, spark)

    # Step 8: ADLS Upload
    AdlsUpload.upload_files_to_blob_storage(conf)

