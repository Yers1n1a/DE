import io
import os
import requests
import pandas as pd
import pyarrow
from google.cloud import storage
from pathlib import Path
from prefect_gcp.cloud_storage import GcsBucket

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
init_url = 'https://nyc-tlc.s3.amazonaws.com/trip+data/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "zoomcamp-ny-taxi")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("bucket-block")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


def web_to_gcs(year, service):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name 
        file_path = Path(f"./data_new/{service}_tripdata_{year}-{month}.csv")

        # download it using requests via a pandas df
        request_url = init_url + str(file_path)

        r = requests.get(request_url)

        pd.DataFrame(io.StringIO(r.text)).to_csv(file_path)

        print(f"Local: {file_path}")

        # read it back into a parquet file
        df = pd.read_csv(file_path)
        file_name = str(file_path).replace('.csv', '.parquet')
        df.to_parquet(file_name, engine='pyarrow')
        print(f"Parquet: {file_name}")

        # upload it to gcs 

        write_gcs(file_path)
        print(f"GCS: {service}/{file_name}")


# web_to_gcs('2019', 'green')
# web_to_gcs('2020', 'green')
# web_to_gcs('2019', 'yellow')
# web_to_gcs('2020', 'yellow')