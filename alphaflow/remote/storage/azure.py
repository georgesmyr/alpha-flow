import os
from typing import Optional 

from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceExistsError
from azure.core.exceptions import ResourceNotFoundError

from alphaflow.utils import load_config
from alphaflow.utils import printc


class AzureBlobStorage(object):

    # Load Blob Service Client from connection string   
    connection_string = load_config()["azure"]["storage"]["connection_string"]
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    def create_container(self, name: str) -> None:
        """ 
        Create a container in the storage account.
        :param name: The name of the new container.
        """
        # Instantiate container client
        container_client = self.blob_service_client.get_container_client(name)
        try:
            container_client.create_container()
            printc(f"\nContainer '{name}' created.", "green")
        except ResourceExistsError as exception:
            raise ResourceExistsError(f"\nContainer '{name}' already exists.") 
        except Exception as exception:
            raise exception


    def delete_container(self, name: str) -> None:
        """
        Delete a container in the storage account.
        :param name: The name of the container to delete.
        """
        # Instantiate container client  
        container_client = self.blob_service_client.get_container_client(name)
        try:
            container_client.delete_container()
            printc(f"\nContainer '{name}' deleted.", "green")
        except ResourceNotFoundError as exception:
            raise ResourceNotFoundError(f"\nContainer {name} does not exist")


    def list_containers(self) -> None:
        """ List containers in the storage account. """
        container_list = self.blob_service_client.list_containers()
        if container_list:
            printc("\nList of containers in the storage account:", "blue")
            for container in container_list:
                printc(f" - {container.name}", "cyan")
        else:
            printc("\nThere are no containers in the storage account.", "blue")


    def upload_block_blob(self, path: str, container_name: str, blob_path: Optional[str] = None) -> None:
        """ 
        Uploads a block blob to the storage container.
        :param path: The path to the file to upload.
        :param container_name: The name of the container.
        :param blob_name: The name of the blob.
        """
        # Instantiate container client
        container_client = self.blob_service_client.get_container_client(container_name)
        # If container does not exist, raise error
        if not container_client.exists():
            raise ResourceNotFoundError(f"Container '{container_name}' does not exist.")
        # If path is not absolute, make it absolute
        if not os.path.isabs(path):
            path = os.path.abspath(path)

        printc("Initiating upload...", "blue")
        # If path points to directory, upload directory
        if os.path.isdir(path):
            for dirpath, _, filenames in os.walk(path):
                for file in filenames:
                    # Get the full file path
                    full_file_path = os.path.join(dirpath, file)
                    # Get the relative file path
                    relative_path = os.path.relpath(full_file_path, path)
                    # Get the blob store path
                    store_path = os.path.join(blob_path, relative_path)
                    # Upload blob 
                    self._upload_file(full_file_path, container_name, store_path)
        else:
            # If path points to file, upload file
            self._upload_file(path, container_name, blob_path)
                

    def _upload_file(self, path: str, container_name: str, blob_store_path: str, verbose: bool = True) -> None:
        """
        Uploads a file to the storage container.
        :param path: The path to the file to upload.
        :param container_name: The name of the container.
        :param blob_name: The name of the blob.
        """
        try:
            # Instantiate blob client
            blob_obj = self.blob_service_client.get_blob_client(container=container_name, blob=blob_store_path)
            # Write blob at blob_store_path
            with open(path, mode='rb') as file_data:
                blob_obj.upload_blob(file_data)
                if verbose:
                    print(f"\033[94mUploaded file\033[0m {path} \033[94mto\033[0m {container_name}/{blob_store_path}")
        # If blob already exists, skip
        except ResourceExistsError as exception:
            raise ResourceExistsError(f"Blob {container_name}/{blob_store_path} already exists.")
            # print(f'Skipped blob {path} because it already exists.')
        except Exception as exception:
            raise exception


    def delete_blob(self, container_name: str, blob_name: str) -> None:
        """ Deletes blob from the storage account. 
        :param container_name: The name of the container.
        :param blob_name: The name of the blob.
        """       
        # Instantiate container client 
        container_client = self.blob_service_client.get_container_client(container_name)
        # If container does not exist, raise error
        if not container_client.exists():
            raise ResourceNotFoundError(f"Container '{container_name}' does not exist.")
        # Instantiate blob client
        blob_client = container_client.get_blob_client(blob_name)
        # If blob does not exist, raise error
        if not blob_client.exists():
            raise ResourceNotFoundError(f"Blob '{blob_name}' does not exist.")
        # Delete blob
        blob_client.delete_blob()
        printc(f"Blob '{blob_name}' deleted.", "green")
    

    def container_exists(self, container_id: str, verbose: bool = True) -> bool:
        """ 
        Check if a container exists in the storage account.
        :param name: Name of the container of which to check the existense.
        """
        # Instantiate container client
        container_client = self.blob_service_client.get_container_client(container_id)
        # Check if container exists
        client_exists = container_client.exists()
        if verbose:
            if client_exists:
                printc(f"Container '{container_id}' exists.", "blue")
            else:
                printc(f"Container '{container_id}' does not exist.", "blue")
        return client_exists


    def list_blobs(self, container_id: str) -> list:
        """ 
        List blobs in the storage account.
        :param container_id: The name of the container.
        """
        # Instantiate container client
        blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        container_client = blob_service_client.get_container_client(container_id)
        # If container does not exist, raise error
        if not container_client.exists():
            raise ResourceNotFoundError(f"Container '{container_id}' does not exist.")
        # List blobs in container
        blob_list = container_client.list_blobs()
        if blob_list:
            printc(f"\nBlobs in container {container_id}:", "blue")
            for blob in blob_list:
                printc(f" - {blob.name}", "cyan")
        else:
            printc("\nContainer is empty.", "blue")
        return blob_list
    