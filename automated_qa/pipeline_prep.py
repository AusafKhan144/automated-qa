from google.cloud import storage
import os
from datetime import datetime

def create_blob_paths(project_name,prev_date,prev_year):
    
    blob_paths = []
    STAGES = ['Consolidation',"Comparison","Standardization","Automatic Validation"]
    for CURRENT_STAGE in STAGES:
        blob_paths.append(f"{project_name}/{CURRENT_STAGE}/{prev_year}/{prev_date}/{project_name}.csv",)

    return blob_paths
    
def upload_file_to_multiple_blobs(bucket_name, source_file_name, blob_paths):
    """
    Uploads a file to multiple blob paths in a GCS bucket.

    Args:
        bucket_name (str): The name of the GCS bucket.
        source_file_name (str): The local path to the file to upload.
        blob_paths (list): A list of blob paths where the file will be uploaded.
    """
    # Initialize the GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for blob_path in blob_paths:
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(source_file_name)
        print(f"File '{source_file_name}' uploaded to '{blob_path}'.")

def get_date_year(date_string):
    try:
        date_obj = datetime.strptime(date_string, "%Y-%m-%d").date()
        return date_obj.strftime("%d%m%Y"), date_obj.strftime("%Y")
    except ValueError:
        exit(f"Not a valid date: '{date_string}'. Expected format: YYYY-MM-DD.")

def prepare_previous_dates_data(API,id, new_file, dataset_date):
    dataset = API.get_datasets_by_id(id)
    project_name = dataset.get('name')
    source_file_name = new_file
    prev_date,prev_year = get_date_year(dataset_date)
    blob_paths = create_blob_paths(project_name,prev_date,prev_year)

    upload_file_to_multiple_blobs(bucket_name, source_file_name, blob_paths)


if __name__ == "__main__":
    # Replace with your GCS bucket name
    bucket_name = 'wmc-data-harvesting-ee530056cd851822'

    # Replace with the local path to your file
    source_file_name = '/home/wmc/WMC/WMCDataHarvesting/James_Hardie/James_Hardie/spiders/JamesHardie_20112024.csv'
    project_name = 'Norton'
    prev_date = '04122024'
    prev_year = '2024'

    # List of blob paths where you want to upload the file
    blob_paths = create_blob_paths(project_name,prev_date,prev_year)

    upload_file_to_multiple_blobs(bucket_name, source_file_name, blob_paths)
