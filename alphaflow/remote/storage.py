import os
from alphaflow.utils import load_config
from alphaflow.utils import printc


class AzureBlobStorage(object):

    # Load connection string from config file    
    connection_string = load_config()["azure"]["storage"]["connection_string"]

    def list_containers(self):
        """ List containers in the storage account. """
        # Instantiate a new BlobServiceClient using a connection string         
        from azure.storage.blob import BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        # List containers in the storage account
        container_list = blob_service_client.list_containers()
        if container_list == []:
            printc("\nList of containers in the storage account:", "blue")
            for container in container_list:
                printc(container.name, "cyan")
        else:
            printc("\nThere are no containers in the storage account.", "blue")

    def container_exists(self, name: str):
        """ Check if a container exists in the storage account. """
        # Instantiate a new BlobServiceClient using a connection string
        from azure.storage.blob import BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        # Instantiate a new ContainerClient
        container_client = blob_service_client.get_container_client(name)
        # Check if the container exists
        client_exists = container_client.exists()
        if client_exists:
            printc(f"Container '{name}' exists.", "blue")
        else:
            printc(f"Container '{name}' does not exist.", "blue")
        return client_exists

    def create_container(self, name: str):
        """ 
        Create a container in the storage account.
        :param name: The name of the new container.
        """
        # Instantiate a new BlobServiceClient using a connection string
        from azure.storage.blob import BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        # Instantiate a new ContainerClient
        container_client = blob_service_client.get_container_client(name)

        try:
            if not container_client.exists():
                # Create new container in the service
                container_client.create_container()
                printc(f"Container '{name}' created.", "green")
                # List containers in the storage account
                self.list_containers()
            else:
                printc(f"Container '{name}' already exists.", "red") 
        except Exception as exception:
            printc("Failed to create container.", "red")
            printc(exception, "red")

    def delete_container(self, name: str):
        """
        Delete a container in the storage account.
        :param name: The name of the container to delete.
        """
        # Instantiate a new BlobServiceClient using a connection string
        from azure.storage.blob import BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        # Instantiate a new ContainerClient
        container_client = blob_service_client.get_container_client(name)

        try:
            # Delete the container
            container_client.delete_container()
            printc(f"Container '{name}' deleted.", "green")
        except Exception as exception:
            printc("Failed to delete container.", "red")
            printc(exception, "red")

    def list_blobs(self, container_name: str):
        """ 
        List blobs in the storage account.
        :param container_name: The name of the container.
        """
        # Instantiate a new BlobServiceClient using a connection string
        from azure.storage.blob import BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        # Instantiate a new ContainerClient
        container_client = blob_service_client.get_container_client(container_name)
        # If container does not exist raise error
        if not container_client.exists():
            raise ValueError(f"Container '{container_name}' does not exist.")
        # List blobs in the container
        blob_list = container_client.list_blobs()
        if blob_list == []:
            printc("\nList of blobs in the container:", "blue")
            for blob in blob_list:
                printc(blob.name, "cyan")
        else:
            printc("\nThere are no blobs in the container.", "blue")

    def delete_blob(self, container_name: str, blob_name: str):
        """ Deletes blob from the storage account. 
        :param container_name: The name of the container.
        :param blob_name: The name of the blob.
        """
        # Instantiate a new BlobServiceClient using a connection string
        from azure.storage.blob import BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        # Instantiate a new ContainerClient
        container_client = blob_service_client.get_container_client(container_name)
        if not container_client.exists():
            raise ValueError(f"Container '{container_name}' does not exist.")
        # Instantiate a new BlobClient
        blob_client = container_client.get_blob_client(blob_name)
        if not blob_client.exists():
            raise ValueError(f"Blob '{blob_name}' does not exist.")
        # Delete the blob
        blob_client.delete_blob()
        printc(f"Blob '{blob_name}' deleted.", "green")

    def upload_block_blob(self, container_name: str, blob_name: str, path: str):
        """ 
        Uploads a block blob to the storage account.
        :param container_name: The name of the container.
        :param blob_name: The name of the blob.
        :param path: The path to the file to upload.
        """
        # Instantiate a new BlobServiceClient using a connection string
        from azure.storage.blob import BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        # Instantiate a new ContainerClient
        container_client = blob_service_client.get_container_client(container_name)
        if not container_client.exists():
            raise ValueError(f"Container '{container_name}' does not exist.")
        # Instantiate a new BlobClient
        blob_client = container_client.get_blob_client(blob_name)
        if not blob_client.exists():
            raise ValueError(f"Blob '{blob_name}' does not exist.")
        # Upload content to the blob
        with open(path, "rb") as data:
            blob_client.upload_blob(data, blob_type="BlockBlob")


        # with open(DEST_FILE, "wb") as my_blob:
        #     download_stream = blob_client.download_blob()
        #     my_blob.write(download_stream.readall())
        # # [END download_a_blob]

        # # [START delete_blob]
        # blob_client.delete_blob()
        # # [END delete_blob]

    # def stream_block_blob(self):

    #     import uuid
    #     # Instantiate a new BlobServiceClient using a connection string - set chunk size to 1MB
    #     from azure.storage.blob import BlobServiceClient, BlobBlock
    #     blob_service_client = BlobServiceClient.from_connection_string(self.connection_string,
    #                                                                    max_single_get_size=1024*1024,
    #                                                                    max_chunk_get_size=1024*1024)

    #     # Instantiate a new ContainerClient
    #     container_client = blob_service_client.get_container_client("containersync")
    #     # Generate 4MB of data
    #     data = b'a'*4*1024*1024

    #     try:
    #         # Create new Container in the service
    #         container_client.create_container()

    #         # Instantiate a new source blob client
    #         source_blob_client = container_client.get_blob_client("source_blob")
    #         # Upload content to block blob
    #         source_blob_client.upload_blob(data, blob_type="BlockBlob")

    #         destination_blob_client = container_client.get_blob_client("destination_blob")
    #         # [START download_a_blob_in_chunk]
    #         # This returns a StorageStreamDownloader.
    #         stream = source_blob_client.download_blob()
    #         block_list = []

    #         # Read data in chunks to avoid loading all into memory at once
    #         for chunk in stream.chunks():
    #             # process your data (anything can be done here really. `chunk` is a byte array).
    #             block_id = str(uuid.uuid4())
    #             destination_blob_client.stage_block(block_id=block_id, data=chunk)
    #             block_list.append(BlobBlock(block_id=block_id))

    #         # [END download_a_blob_in_chunk]

    #         # Upload the whole chunk to azure storage and make up one blob
    #         destination_blob_client.commit_block_list(block_list)

    #     finally:
    #         # Delete container
    #         container_client.delete_container()

    # def page_blob_sample(self):

    #     # Instantiate a new BlobServiceClient using a connection string
    #     from azure.storage.blob import BlobServiceClient
    #     blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)

    #     # Instantiate a new ContainerClient
    #     container_client = blob_service_client.get_container_client("mypagecontainersync")

    #     try:
    #         # Create new Container in the Service
    #         container_client.create_container()

    #         # Instantiate a new BlobClient
    #         blob_client = container_client.get_blob_client("mypageblob")

    #         # Upload content to the Page Blob
    #         data = b'abcd'*128
    #         blob_client.upload_blob(data, blob_type="PageBlob")

    #         # Download Page Blob
    #         with open(DEST_FILE, "wb") as my_blob:
    #             download_stream = blob_client.download_blob()
    #             my_blob.write(download_stream.readall())

    #         # Delete Page Blob
    #         blob_client.delete_blob()

    #     finally:
    #         # Delete container
    #         container_client.delete_container()

    # def append_blob_sample(self):

    #     # Instantiate a new BlobServiceClient using a connection string
    #     from azure.storage.blob import BlobServiceClient
    #     blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)

    #     # Instantiate a new ContainerClient
    #     container_client = blob_service_client.get_container_client("myappendcontainersync")

    #     try:
    #         # Create new Container in the Service
    #         container_client.create_container()

    #         # Instantiate a new BlobClient
    #         blob_client = container_client.get_blob_client("myappendblob")

    #         # Upload content to the Page Blob
    #         with open(SOURCE_FILE, "rb") as data:
    #             blob_client.upload_blob(data, blob_type="AppendBlob")

    #         # Download Append Blob
    #         with open(DEST_FILE, "wb") as my_blob:
    #             download_stream = blob_client.download_blob()
    #             my_blob.write(download_stream.readall())

    #         # Delete Append Blob
    #         blob_client.delete_blob()

    #     finally:
    #         # Delete container
    #         container_client.delete_container()
