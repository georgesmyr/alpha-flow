from alphaflow.remote.blob_storage import AzureBlobStorage

blob_storage = AzureBlobStorage()
blob_storage.list_containers()
# blob_storage.delete_container("ml4t")
# blob_storage.create_container("ml4t")
# dir = "/Users/georgesmyridis/Desktop/Projects/machine-learning-for-trading/data"
# blob_storage.upload_block_blob(f"{dir}/stooq/", "ml4t", "data")
# # blob_storage.delete_blob("ml4t", "data/.DS_Store")
# blob_storage.list_blobs("ml4t")

