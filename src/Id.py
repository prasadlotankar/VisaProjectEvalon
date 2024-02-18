from pyspark.sql.functions import *
from datetime import datetime
import os
import logging


class IdDriver:
    """Class for unique id the file"""

    def __init__(self, spark_session):
        self.spark = spark_session

    def process_data_uuid(self, spark):
        try:
            # Configure the logger
            log_filename = os.path.join(self['Paths']['log'],
                                        f"log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt")
            logging.basicConfig(filename=log_filename, level=logging.INFO,
                                format='%(asctime)s - %(levelname)s - %(message)s')

            # Set HADOOP_HOME environment variable
            # Input path
            input_path = self['Paths']['decrypted_output_file']

            # Read the JSON file into a DataFrame
            df = spark.read.json(input_path)

            # Add a new column "sample_UUID" using withColumn and concat functions
            df = df.withColumn("sample_UUID",
                               concat(
                                   lit("visa_"),
                                   col(self['Paths']['uuid_column']),
                                   lit("_"),
                                   lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                               )
                               )

            # Show the DataFrame with the new column
            df.show(truncate=False)

            # Write the original DataFrame using Spark in JSON format
            df.write.format('json').mode('overwrite').save(self['Paths']['uuid_output_path'])

            # Log the information
            logging.info(
                f"Data processing with UUID completed. Output saved at: {self['Paths']['uuid_output_path']}")

        except Exception as e:
            # Log the exception
            logging.error(f"An error occurred: {str(e)}")
            # Optionally, you can raise the exception again if needed
            # raise e

# Example usage:
# id_driver = IdDriver(spark_session)
# id_driver.process_data_uuid(get_config_details, spark)
