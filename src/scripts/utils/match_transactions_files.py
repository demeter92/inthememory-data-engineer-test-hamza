from typing import Optional, List
from azure.storage.blob import ContainerClient
from config.spark_config import ACCOUNT_NAME, CONTAINER_NAME, ACCOUNT_KEY

def list_matching_transaction_files_azure(
    date: Optional[str] = None,
    hour: Optional[str] = None
) -> List[str]:
   
    connection_string = f"DefaultEndpointsProtocol=https;AccountName={ACCOUNT_NAME};AccountKey={ACCOUNT_KEY};EndpointSuffix=core.windows.net"
    container_client = ContainerClient.from_connection_string(
        conn_str=connection_string,
        container_name=CONTAINER_NAME
    )

    if date and hour:
        # Filter for files with both date and hour
        name_starts_with = f"transactions_{date}_{hour}"
    elif date:
        # Filter for files with only the date
        name_starts_with = f"transactions_{date}"
    else:
        # If no date is provided, match all files with the format transactions_{date isoformat}_{hour}
        name_starts_with = "transactions_"
    blobs = container_client.list_blobs(name_starts_with=name_starts_with)    
    base_url= f"wasbs://{CONTAINER_NAME}@{ACCOUNT_NAME}.blob.core.windows.net/"
    result = [(base_url + blob.name) for blob in blobs]
    return result
