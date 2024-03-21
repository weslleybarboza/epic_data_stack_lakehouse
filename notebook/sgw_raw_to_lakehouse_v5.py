# %%
#partition by datetime

# %%
import boto3

# %%
# ip and environments
environment = 'prd'

# Set the bucket and folder paths
source_bucket = 'landing-zone'
source_folder = 'files/pscore/ascll'

lakehouse_bucket = 'lakehouse' 
lakehouse_folder = 'bronze'

# table destination settings
dest_db_catalog = 'iceberg'
dest_db_schema = 'pscore'
dest_db_table = 'sgw'
dest_final_db = f'{dest_db_catalog}.{dest_db_schema}'
dest_final_table = f'{dest_final_db}.{dest_db_table}'

# Spark identification and settings
appname = 'SGW_from_landing_to_bronze'
log_level = 'WARN' # Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN

# Set your MinIO credentials
s3_endpoint = 'http://minio:9000'
s3_access_key = 'minio'
s3_secret_key = 'minio123'

# %%
import pyspark
# import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import year, to_date, month, dayofmonth,  from_unixtime, unix_timestamp


# %%
spark = SparkSession.builder\
    .appName(appname)\
    .getOrCreate()

# %%
spark.sparkContext.setLogLevel(log_level)
print(pyspark.SparkConf().getAll())

print(spark.sparkContext.getConf().get("spark.sql.catalog.iceberg.warehouse"))

# %%
schema = StructType([
                    StructField("record_type", StringType()),
                    StructField("network_initiated_pdp_context", StringType()),
                    StructField("imsi", StringType()),
                    StructField("msisdn", StringType()),
                    StructField("imei", StringType()),
                    StructField("charging_id", StringType()),
                    StructField("ggsn_pgw_address", StringType()),
                    StructField("sgsn_sgw_address", StringType()),
                    StructField("ms_nw_capability", StringType()),
                    StructField("pdp_pdn_type", StringType()),
                    StructField("served_pdp_address", StringType()),
                    StructField("dynamic_address_flag", StringType()),
                    StructField("access_point_name_ni", StringType()),
                    StructField("record_sequence_number", StringType()),
                    StructField("record_sequence_number_meg", StringType()),
                    StructField("node_id", StringType()),
                    StructField("local_sequence_number", StringType()),
                    StructField("charging_characteristics", StringType()),
                    StructField("record_opening_time", StringType()),
                    StructField("duration", StringType()),
                    StructField("rat_type", StringType()),
                    StructField("cause_for_record_closing", StringType()),
                    StructField("diagnostic", StringType()),
                    StructField("volume_uplink", StringType()),
                    StructField("volume_downlink", StringType()),
                    StructField("total_volume", StringType()),
                    StructField("lac_or_tac", StringType()),
                    StructField("ci_or_eci", StringType()),
                    StructField("rac", StringType()),
                    StructField("rnc_unsent_data_volume", StringType()),
                    StructField("req_alloc_ret_priority", StringType()),
                    StructField("neg_alloc_ret_priority", StringType()),
                    StructField("req_traffic_class", StringType()),
                    StructField("neg_traffic_class", StringType()),
                    StructField("qci", StringType()),
                    StructField("req_max_bitrate_uplink", StringType()),
                    StructField("req_max_bitrate_downlink", StringType()),
                    StructField("req_guar_bitrate_uplink", StringType()),
                    StructField("req_guar_bitrate_downlink", StringType()),
                    StructField("neg_max_bitrate_uplink", StringType()),
                    StructField("neg_max_bitrate_downlink", StringType()),
                    StructField("neg_guar_bitrate_uplink", StringType()),
                    StructField("neg_guar_bitrate_downlink", StringType()),
                    StructField("mccmnc", StringType()),
                    StructField("country_name", StringType()),
                    StructField("input_filename", StringType()),
                    StructField("output_filename", StringType()),
                    StructField("event_date", TimestampType())
                ])

# %%
spark.sql(f"""
CREATE DATABASE IF NOT EXISTS iceberg.raw COMMENT '' LOCATION 's3a://datalake/iceberg/raw/'
""")

# %%
table = spark.sql(f"""
        CREATE TABLE IF NOT EXISTS iceberg.raw.{dest_db_table}
        (
            record_type STRING,
            network_initiated_pdp_context STRING,
            imsi STRING,
            msisdn STRING,
            imei STRING,
            charging_id STRING,
            ggsn_pgw_address STRING,
            sgsn_sgw_address STRING,
            ms_nw_capability STRING,
            pdp_pdn_type STRING,
            served_pdp_address STRING,
            dynamic_address_flag STRING,
            access_point_name_ni STRING,
            record_sequence_number STRING,
            record_sequence_number_meg STRING,
            node_id STRING,
            local_sequence_number STRING,
            charging_characteristics STRING,
            record_opening_time STRING,
            duration STRING,
            rat_type STRING,
            cause_for_record_closing STRING,
            diagnostic STRING,
            volume_uplink STRING,
            volume_downlink STRING,
            total_volume STRING,
            lac_or_tac STRING,
            ci_or_eci STRING,
            rac STRING,
            rnc_unsent_data_volume STRING,
            req_alloc_ret_priority STRING,
            neg_alloc_ret_priority STRING,
            req_traffic_class STRING,
            neg_traffic_class STRING,
            qci STRING,
            req_max_bitrate_uplink STRING,
            req_max_bitrate_downlink STRING,
            req_guar_bitrate_uplink STRING,
            req_guar_bitrate_downlink STRING,
            neg_max_bitrate_uplink STRING,
            neg_max_bitrate_downlink STRING,
            neg_guar_bitrate_uplink STRING,
            neg_guar_bitrate_downlink STRING,
            mccmnc STRING,
            country_name STRING,
            input_filename STRING,
            output_filename STRING,
            event_date Timestamp
        ) 
        USING iceberg
        PARTITIONED BY (event_date)
        """)

# %%
s3 = boto3.client('s3', endpoint_url=s3_endpoint, aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key)

# %%
# List all files in the source directory
file_list = []
paginator = s3.get_paginator('list_objects_v2')

for result in paginator.paginate(Bucket=f"{environment}-{source_bucket}", Prefix=source_folder):
    
    if 'Contents' in result:
        for item in result['Contents']:
            file_list.append(item['Key'])


# %%
# reading files in the source
for file_name in file_list:

    print(f'File in processing: {file_name}')
    
    df = spark.read.format("csv") \
                    .option("header", "false") \
                    .option("delimiter", ";") \
                    .schema(schema) \
                    .load(f"s3a://{environment}-{source_bucket}/{file_name}")
    # some transformations
    df = df.withColumn("duration", df["duration"].cast("double"))
    # df = df.withColumn("event_date", to_date(df["record_opening_time"], "yyyyMMddHHmmss"))
    # to_date(df["record_opening_time"], "yyyyMMddHHmmss")

    # df.withColumn("event_date", from_unixtime(unix_timestamp("record_opening_time", "yyyyMMddHHmmss")))

    df.select('imsi').show()
    
    # wrinte the data on lakehouse
    df.writeTo(f'iceberg.raw.{dest_db_table}').append()     


# %%
tb = spark.table(f'iceberg.raw.{dest_db_table}')
tb.printSchema()

# %%
print(f"No of Records: {tb.count()}")


