from azure.storage.blob import BlobServiceClient
import os
import configparser


class AdlsUpload:
    """Class for Encrypting the file"""

    def __init__(self, spark_session):
        self.spark = spark_session

    def upload_files_to_blob_storage(get_config_details):
        def upload_to_blob_storage(file_path, file_name):
            try:
                blob_service_client = BlobServiceClient.from_connection_string(connection_string)

                blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)

                # Check if the blob already exists
                if blob_client.exists():
                    print(f"File {file_name} already exists in Azure Blob Storage")
                else:
                    with open(file_path, "rb") as data:
                        blob_client.upload_blob(data)
                    print(f"Uploaded file {file_name}")
            except Exception as e:
                print(f"Error uploading file {file_name}: {str(e)}")

        connection_string = get_config_details['azure_storage']['connection_string']
        container_name = get_config_details['azure_storage']['container_name']
        output_directory = get_config_details['Paths']['qc']

        # ADLS Upload for each CSV, JSON, and TXT file generated by Spark
        allowed_extensions = [".csv", ".json", ".txt"]

        for file_name in os.listdir(output_directory):
            if any(file_name.endswith(ext) for ext in allowed_extensions):
                file_path = os.path.join(output_directory, file_name)
                #upload_to_blob_storage(file_path, file_name)

# nneds to be converted in spark
