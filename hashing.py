from pyspark.sql.functions import *
from pyspark.sql.types import StringType
import requests


class Hashing:
    """Class for unique id the file"""

    def __init__(self, spark_session):
        self.spark = spark_session

        def send_data_and_get_hash(self, spark):
            api_url = self['Paths']['apiurl']
            # Prepare data for the request
            payload = {'data': self['Paths']['decrypted_output_file']}
            # Send POST request to the API
            response = requests.post(api_url, json=payload)
            # Parse the JSON response
            result = response.json()
            if 'hash_value' in result:
                return result['hash_value']
            elif 'error' in result:
                raise Exception(result['error'])
            else:
                raise Exception('Unexpected response from the server')

        hash_udf = udf(send_data_and_get_hash, StringType())

        def process_data_and_save_json(self, spark,columns, drop_columns, output_file):
            input_path = self['Paths']['decrypted_output_file']
            df = spark.read.json(input_path)

            # Calculate hash values for specified columns
            for cold in columns:
                print(cold)
                print("hiiiiii")
                df = df.withColumn(f'Hash_Value_of_{cold}', hash_udf(df[cold]))

            # Drop specified columns
            df = df.drop(col(drop_columns))

            df.show()

            # Save DataFrame to JSON file
            df.write.json(output_file)

        # Example usage

        file_path_or_data = self['Paths']['decrypted_output_file']  # input file as decripted json in this case
        columns = self['Paths']['hashing_column_names']
        drop_columns = self['Paths']['hashing_column_names']
        output_file = self['Paths']['hashed_output_file']  # hasing output as input to uuid

        process_data_and_save_json(file_path_or_data, columns, drop_columns, output_file)

        # Stop SparkSession
        spark.stop()
