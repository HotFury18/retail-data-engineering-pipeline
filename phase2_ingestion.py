from azure.storage.blob import BlobServiceClient
import os

# This is the standard default connection string Microsoft provides for Azurite
connection_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"

print("Connecting to local Azurite emulator...")
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# 1. Create the Container (The Folder)
container_name = "raw-data"
try:
    container_client = blob_service_client.create_container(container_name)
    print(f"--> Container '{container_name}' created successfully.")
except Exception as e:
    print(f"--> Container '{container_name}' already exists, skipping creation.")
    container_client = blob_service_client.get_container_client(container_name)

# 2. Upload the Files
files_to_upload = ["dim_employees.csv", "raw_sales_2026_03.csv"]

for file_name in files_to_upload:
    # Check if the file actually exists on your computer first
    if os.path.exists(file_name):
        # Create a client for the specific file
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
        
        print(f"--> Uploading {file_name} into '{container_name}'...")
        with open(file_name, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        print(f"    Success!")
    else:
        print(f"--> ERROR: {file_name} not found in this folder.")

print("Phase 2 Ingestion Complete!")