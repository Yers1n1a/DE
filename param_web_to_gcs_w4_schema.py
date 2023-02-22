from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta
from google.cloud import storage
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa
import requests

storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB


# @task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
# def fetch(dataset_url):
#     """Read taxi data from web into pandas DataFrame"""

#     r = requests.get(dataset_url)
#     pd.DataFrame(io.StringIO(r.text)).to_csv(file_path)

#     df = pd.read_csv(dataset_url)
#     table = pv.read_csv(dataset_url)
#     return table


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url, index_col=0)
    return df


@task()
def write_local_csv(df, dataset_file):
    """Write DataFrame out locally as csv file"""
    path = Path(f"data_new_schema/{dataset_file}.csv")
    if not path.parent.is_dir():
        path.parent.mkdir(parents=True)
    df.to_csv(path)
    return path


@task(log_prints=True)

def schema_transform_to_parquet(path, color):
    """Fix dtype issues"""

    table_schema_green = pa.schema(
    [
        ('VendorID',pa.string()),
        ('lpep_pickup_datetime',pa.timestamp('s')),
        ('lpep_dropoff_datetime',pa.timestamp('s')),
        ('store_and_fwd_flag',pa.string()),
        ('RatecodeID',pa.int64()),
        ('PULocationID',pa.int64()),
        ('DOLocationID',pa.int64()),
        ('passenger_count',pa.int64()),
        ('trip_distance',pa.float64()),
        ('fare_amount',pa.float64()),
        ('extra',pa.float64()),
        ('mta_tax',pa.float64()),
        ('tip_amount',pa.float64()),
        ('tolls_amount',pa.float64()),
        ('ehail_fee',pa.float64()),
        ('improvement_surcharge',pa.float64()),
        ('total_amount',pa.float64()),
        ('payment_type',pa.int64()),
        ('trip_type',pa.int64()),
        ('congestion_surcharge',pa.float64()),
    ]
)

    table_schema_yellow = pa.schema(
   [
        ('VendorID', pa.string()), 
        ('tpep_pickup_datetime', pa.timestamp('s')), 
        ('tpep_dropoff_datetime', pa.timestamp('s')), 
        ('passenger_count', pa.int64()), 
        ('trip_distance', pa.float64()), 
        ('RatecodeID', pa.string()), 
        ('store_and_fwd_flag', pa.string()), 
        ('PULocationID', pa.int64()), 
        ('DOLocationID', pa.int64()), 
        ('payment_type', pa.int64()), 
        ('fare_amount',pa.float64()), 
        ('extra',pa.float64()), 
        ('mta_tax', pa.float64()), 
        ('tip_amount', pa.float64()), 
        ('tolls_amount', pa.float64()), 
        ('improvement_surcharge', pa.float64()), 
        ('total_amount', pa.float64()), 
        ('congestion_surcharge', pa.float64())]

)

    table = pv.read_csv(path)

    if color == 'yellow':
        table = table.cast(table_schema_yellow)
    
    elif color == 'green':
        table = table.cast(table_schema_green)

    print(f"rows: {len(table)}")
    return table


@task()
def write_local_pq(table, color, dataset_file):
    """Write DataFrame out locally as parquet file"""
    path = Path(f"data_new_schema/{dataset_file}.parquet")
    if not path.parent.is_dir():
        path.parent.mkdir(parents=True)
    pq.write_table(table, path)
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("bucket-block")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    # https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz

    df = fetch(dataset_url)
    path_csv = write_local_csv(df, dataset_file)
    df_clean = schema_transform_to_parquet(path_csv, color)
    path = write_local_pq(df_clean, color, dataset_file)
    write_gcs(path)


@flow()
def etl_parent_flow(
    months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"
):
    for month in months:
        etl_web_to_gcs(year, month, color)


if __name__ == "__main__":
    color = "yellow"
    months = [1,2,3,4,5,6,7,8,9,10,11,12]
    year = 2020
    etl_parent_flow(months, year, color)

# ,2,3,4,5,6,7,8,9,10,11,12