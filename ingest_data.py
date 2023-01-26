import pyarrow.parquet as pq
import pandas as pd
from sqlalchemy import create_engine
import argparse
import os


def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db_name = params.db_name
    table_name = params.table_name
    url_data = params.url_data
    data_file = 'yellow_tripdata_2021-01.parquet'
    # data_file = 'yellow_tripdata_2019-01.parquet'

    url_zones = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"

    # wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
    # wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-01.parquet
    # wget https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv -o taxi_zones.csv
    
    # Uncomment in case of downloading
    # os.system(f'wget {url_data} -O {data_file}')
    # os.system(f'wget {url_zones} -O taxi_zones.csv')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')

    trips = pq.read_table(f'{data_file}')
    df = trips.to_pandas()

    zones = pd.read_csv('taxi_zones.csv')

    zones.to_sql(name='zones', con=engine, if_exists='replace')
    print('zones added')

    df.head(0).to_sql(name=f'{table_name}', con=engine, if_exists='replace')
    df.to_sql(name=f'{table_name}', con=engine, if_exists='append', chunksize=100000)

    print('Ingesting finished')


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingesting parquet to Postgres')

    parser.add_argument('--user', help='Username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db_name', help='db_name for postgres')
    parser.add_argument('--table_name', help='table_name where we will write results to in postgres')
    parser.add_argument('--url_data', help='url with data')

    args = parser.parse_args()

    main(args)
