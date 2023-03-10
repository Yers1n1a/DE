StructType(
    [
        types.StructField("dispatching_base_num", types.StringType(), True),
        types.StructField("pickup_datetime", types.TimestampType(), True),
        types.StructField("dropoff_datetime", types.TimestampType(), True),
        types.StructField("PULocationID", types.IntegerType(), True),
        types.StructField("DOLocationID", types.IntegerType(), True),
        types.StructField("SR_Flag", types.StringType(), True),
        types.StructField("Affiliated_base_number", types.StringType(), True)
    ]
)
