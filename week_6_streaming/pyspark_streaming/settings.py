import pyspark.sql.types as T

INPUT_DATA_PATH_GREEN = "../../data_new_schema/green_tripdata_2019-01.csv"
INPUT_DATA_PATH_FHV = "../../data/fhv/fhv_tripdata_2019-01.csv.gz"

BOOTSTRAP_SERVERS = "localhost:9092"

# TOPIC_WINDOWED_VENDOR_ID_COUNT = 'vendor_counts_windowed'

PRODUCE_TOPIC_RIDES_CSV = CONSUME_TOPIC_RIDES_CSV = "green_rides_csv"
PRODUCE_TOPIC_RIDES_FHV_CSV = CONSUME_TOPIC_RIDES_FHV_CSV = "fhv_rides_csv"

GREEN_RIDE_SCHEMA = T.StructType(
    [
        T.StructField("VendorID", T.IntegerType()),
        T.StructField("lpep_pickup_datetime", T.TimestampType()),
        T.StructField("lpep_dropoff_datetime", T.TimestampType()),        
        T.StructField("store_and_fwd_flag", T.StringType()),
        T.StructField("RatecodeID", T.IntegerType()),
        T.StructField("PULocationID", T.IntegerType()),
        T.StructField("DOLocationID", T.IntegerType()),
        T.StructField("passenger_count", T.IntegerType()),
        T.StructField("trip_distance", T.FloatType()),
        T.StructField("fare_amount", T.FloatType()),
        T.StructField("extra", T.FloatType()),
        T.StructField("mta_tax", T.FloatType()),
        T.StructField("tip_amount", T.FloatType()),
        T.StructField("tolls_amount", T.FloatType()),
        T.StructField("ehail_fee", T.FloatType()),
        T.StructField("improvement_surcharge", T.FloatType()),
        T.StructField("total_amount", T.FloatType()),
        T.StructField("payment_type", T.IntegerType()),
        T.StructField("trip_type", T.FloatType()),
        T.StructField("congestion_surcharge", T.FloatType())
    ]
)

FHV_RIDE_SCHEMA = T.StructType(
    [
        T.StructField("dispatching_base_num", T.StringType(), True),
        T.StructField("pickup_datetime", T.TimestampType(), True),
        T.StructField("dropoff_datetime", T.TimestampType(), True),
        T.StructField("PULocationID", T.IntegerType(), True),
        T.StructField("DOLocationID", T.IntegerType(), True),
        T.StructField("SR_Flag", T.StringType(), True),
        T.StructField("Affiliated_base_number", T.StringType(), True),
    ]
)


# RIDE_SCHEMA = T.StructType(
#     [
#         T.StructField("vendor_id", T.IntegerType()),
#         T.StructField("tpep_pickup_datetime", T.TimestampType()),
#         T.StructField("tpep_dropoff_datetime", T.TimestampType()),
#         T.StructField("passenger_count", T.IntegerType()),
#         T.StructField("trip_distance", T.FloatType()),
#         T.StructField("payment_type", T.IntegerType()),
#         T.StructField("total_amount", T.FloatType()),
#     ]
# )