import os
from azure.storage.blob import BlobServiceClient

# Lấy connection string từ biến môi trường
connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
if not connection_string:
    raise ValueError("Please set AZURE_STORAGE_CONNECTION_STRING environment variable")

# Tên container và file
container_name = "manhvd-container"
local_file_path = "test.txt"
blob_name = "test.txt"

try:
    # Tạo BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Lấy container client
    container_client = blob_service_client.get_container_client(container_name)

    # Tạo container nếu chưa tồn tại
    if not container_client.exists():
        container_client.create_container()

    # Upload file
    with open(local_file_path, "rb") as data:
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(data, overwrite=True)
        print(f"File {local_file_path} đã được upload thành công!")

except Exception as e:
    print(f"Lỗi khi upload file: {str(e)}") 