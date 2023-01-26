FROM python:3.9

RUN apt-get install wget
RUN pip install pandas 
RUN pip install sqlalchemy 
RUN pip install pyarrow 
RUN pip install psycopg2

WORKDIR /app

COPY ingest_data.py ingest_data.py
COPY yellow_tripdata_2021-01.parquet yellow_tripdata_2021-01.parquet
COPY taxi_zones.csv taxi_zones.csv

ENTRYPOINT [ "python", "ingest_data.py" ]