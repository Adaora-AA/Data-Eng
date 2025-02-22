import os
import requests
import gzip
import io
from google.cloud import storage  
import dlt
from dlt.sources.filesystem import filesystem, read_csv

# Set environment variables
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/adaora/terra/keys/my-creds.json"

# Constants
BUCKET_NAME = "adaoraah-terra-bucket"
BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"

# Define taxi types and date ranges
TAXI_TYPES = {
    "yellow": {"years": range(2019, 2021), "folder": "yellow_trips"},
    "green": {"years": range(2019, 2021), "folder": "green_trips"},
    "fhv": {"years": [2019], "folder": "fhv_trips"},
}

# Google Cloud Storage client
client = storage.Client()


def upload_to_gcs_from_url(bucket_name, object_name, file_url):
    
    try:
        response = requests.get(file_url, stream=True)
        response.raise_for_status()

        # Decompress the file
        with gzip.GzipFile(fileobj=io.BytesIO(response.content), mode="rb") as f:
            csv_data = f.read()

        # Upload to GCS
        print(f"Uploading {object_name} to GCS...")
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_name)

        with io.BytesIO(csv_data) as csv_buffer:
            blob.upload_from_file(csv_buffer, content_type="text/csv", timeout=300)

        print(f"Upload successful: gs://{bucket_name}/{object_name}")
    except requests.RequestException as e:
        print(f"Failed to download {file_url}: {e}")
    except Exception as e:
        print(f"Error uploading {object_name} to GCS: {e}")


def process_taxi_data():
    for taxi_type, config in TAXI_TYPES.items():
        for year in config["years"]:
            for month in range(1, 13):
                file_name = f"{taxi_type}/{taxi_type}_tripdata_{year}-{month:02d}.csv.gz"
                file_url = BASE_URL + file_name
                object_name = f"{config['folder']}/{taxi_type}_{year}-{month:02d}.csv"

                upload_to_gcs_from_url(BUCKET_NAME, object_name, file_url)


def load_data_into_bigquery():
    for taxi_type, config in TAXI_TYPES.items():
        bucket_path = f"gs://{BUCKET_NAME}/{config['folder']}"
        dataset_name = "taxi_trips"

        try:
            filesystem_source = filesystem(bucket_url=bucket_path, file_glob="*.csv") | read_csv()
            pipeline = dlt.pipeline(
                pipeline_name=f"{taxi_type}_taxi_data",
                destination="bigquery",
                dataset_name=dataset_name,
            )
            info = pipeline.run(filesystem_source.with_name(f"{taxi_type}_taxi_data"))
            print(f"Load job completed for {taxi_type}: {info}")
        except Exception as e:
            print(f"Error loading {taxi_type} data into BigQuery: {e}")


if __name__ == "__main__":
    process_taxi_data()
    load_data_into_bigquery()
