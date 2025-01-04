

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

class AzureClient:
    def __init__(self, storage_account_name, storage_account_url, storage_account_key):
        ##Create a Client Object
        self.storage_account_name = storage_account_name
        self.storage_account_url = storage_account_url
        self.storage_account_key = storage_account_key
        self.blob_client_conn = self.get_connection_to_blob_client()
        
    def get_connection_to_blob_client(self):
        """
        Used to connect to azure blob service. If there is a proper
        connection it says connection successful else
        it tries to throw an error
        Returns:
            - returns the azure blob connection
        """
        print(
            "Establishing connection to Azure Blob service using account "
            "name: {}".format(self.storage_account_name)
        )
        
        try:
            
            print(self.storage_account_url, self.storage_account_key)
            self.blob_client_conn = BlobServiceClient(
                account_url=self.storage_account_url,
                credential=self.storage_account_key)
            print("Connection successful to blob")
        except Exception as e:
            print(
                "ERROR: There seems to be an error in connecting to the "
                "Azure Blob Service!!", f"{e}"
            )
            
        return self.blob_client_conn

    def create_container(self, container_name):
        """
        Used to Create a Container, if the container exists, it will not create again.
        """
        try:
            self.blob_client_conn.create_container(name=container_name)
            print("The container {} has been created.".format(container_name))
                
        except ResourceExistsError :
            print("The container with given name: {} already exists. Not creating again.".format(container_name)
            )

        return container_name
            
    def upload_file(self, container_name, blob_name, file_path):
        """
        Used to upload the locally created files to azure blobs
        container_name: Container name into which the data has to be uploaded
        blob_name: blob name in to which the data has to be uploaded
        file_path: file path that has to be uploaded
        :return: None
        """

        self.blob_conn = self.blob_client_conn.get_blob_client(container=container_name, blob=blob_name)

        with open(file_path, 'rb') as data:
            self.blob_conn.upload_blob(data, overwrite=True)
        # self.conn.create_blob_from_path(container_name, blob_name, file_path)
        print("Uploaded the file into blob - {}".format(blob_name))
        
