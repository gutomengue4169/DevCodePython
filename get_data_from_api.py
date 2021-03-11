# Load libraries
from azure.storage.blob import BlobServiceClient
import requests 
from requests.exceptions import HTTPError
import json
from datetime import datetime
import os
import urllib.parse as urlparse

# Define parameters
storageAccountURL = "https://datalakestorageescola.blob.core.windows.net/datalakesourcedata"
storageKey         = "kZoGlxzygrJ1ZV1TlM0LMmubkWR5WOk6AOnV8FSJ+iNpePhVXSRUo1az6gWxvlIntRK8Yo654zUbikM9jrBmIA=="
containerName      = "registro_escolas/dados_" + datetime.today().strftime('%Y-%m-%d_%H.%M.%S')

# Establish connection with the blob storage account
blob_service_client = BlobServiceClient(account_url=storageAccountURL,
                               credential=storageKey
                               )

# Start a request session and container client
session = requests.Session()
container_client = blob_service_client.get_container_client(containerName)
end_url = "/api/3/action/datastore_search?resource_id=5579bc8e-1e47-47ef-a06e-9f08da28dec8"
i = 0 

### get the offset value from url
def get_offset (url):
    parsed = urlparse.urlparse(url)
    return int (urlparse.parse_qs(parsed.query)['offset'][0])


while True:
    i += 1
    url = "https://dadosabertos.poa.br" + end_url
    try:
        page = session.get(url).json()
        print ('Working on request ' + url) 
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')  
    except Exception as err:
        print(f'Other error occurred: {err}')  
    else:
        with open('data.json', 'w') as f:
            json.dump(page, f)        
        with open("data.json", "rb") as data:
            blob_client = container_client.upload_blob(name="file " + str(i) + ".json", data=data)
            print ('uploading file '+ str(i) + ".json")
        if int(page["result"]["total"]) <= get_offset(page["result"]["_links"]["next"]):
            break 
        end_url = page["result"]["_links"]["next"]

os.remove("data.json")
