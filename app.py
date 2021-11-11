from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import pandas as pd
from io import StringIO
STORAGEACCOUNTURL = "https://switchprime.blob.core.windows.net"
STORAGEACCOUNTKEY = "LQY4OaXSItUGIVSzVRMwDZgQ46sIa9D+YZ0aM9AIsY3lY/nLLjjMlzSPWRtx0A9xEnrqSIw5oCMAX5cehgWXyg==>"
CONTAINERNAME = "cont"
BLOBNAME = "item.txt"

blob_service_client = BlobServiceClient(
    account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)

blob_client_instance = blob_service_client.get_blob_client(
    CONTAINERNAME, BLOBNAME, snapshot=None)

blob_data = blob_client_instance.download_blob()
df = pd.read_csv(StringIO(blob_data.content_as_text()))
df.drop(df[(df['whs']!='Q')].index, inplace=True)

df.drop(df[(df['whs']!='Q')].index, inplace=True)

container_client = blob_service_client.get_container_client('cont')
# Instantiate a new BlobClient
blob_client = container_client.get_blob_client("output.csv")
# upload data
blob_client.upload_blob(str(df.to_csv()), blob_type="BlockBlob")
