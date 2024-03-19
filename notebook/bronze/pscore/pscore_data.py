# %% [markdown]
# ### Libraries and session

# %%
import pyspark
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DateType, DecimalType
from pyspark.sql.functions import year, to_date, month, dayofmonth,  from_unixtime, unix_timestamp

# %%
# ip and environments
environment = 'prd'

# Source
system_source = "pscore"
system_table ="pscore_sgw"

# Set the bucket and folder paths
source_bucket = 'landing-zone'
source_folder = f'files/{system_source}/ascll'

lakehouse_bucket = 'lakehouse'
lakehouse_folder = 'iceberg'

# table destination settings
dest_db_catalog = 'iceberg'
dest_db_schema = 'bronze'
dest_db_table = system_table
dest_final_db = f'{dest_db_catalog}.{dest_db_schema}'
dest_final_table = f'{dest_final_db}.{dest_db_table}'

# Spark identification and settings
appname = f'BRONZE_{dest_final_db}.{dest_final_table}'
log_level = 'WARN' # Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN

# Set your MinIO credentials
s3_endpoint = 'http://minio:9000'
s3_access_key = 'minio'
s3_secret_key = 'minio123'

# %%
spark = SparkSession.builder\
    .appName(appname)\
    .getOrCreate()

# %%
print("=================================================")
spark.sparkContext.setLogLevel(log_level)
print(pyspark.SparkConf().getAll())

# %% [markdown]
# ### Read from the source

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


# %% [markdown]
# #### Data Contract

# %%
df_source_schema = StructType([
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
num_columns_contract = len(df_source_schema.fields)
print("Number of columns of contract:", num_columns_contract)

# %%
df_source_data = spark.createDataFrame([], schema=df_source_schema)

# %%
# reading files in the source
for file_name in file_list:

    print(f'File in processing: {file_name}')
    
    df = spark.read.format("csv") \
                    .option("header", "true") \
                    .option("delimiter", ";") \
                    .schema(df_source_schema) \
                    .load(f"s3a://{environment}-{source_bucket}/{file_name}")
    
    # df.show(5)
    
    if len(df.columns) == num_columns_contract:
        print('No of columns matched')
        df_source_data = df_source_data.union(df)

# %%
# print("No of lines to load: ", len(df_source_data))
# len(df_source_data.columns)

# %%
# df_source_data.describe()

# %% [markdown]
# ### DDL on lakehouse

# %% [markdown]
# #### Data base

# %%
##creating db
sql_db_create = f"""
CREATE DATABASE IF NOT EXISTS {dest_final_db} COMMENT '' LOCATION 's3a://{environment}-{lakehouse_bucket}/{dest_db_catalog}/{dest_db_schema}/'
"""
print(sql_db_create)
spark.sql(sql_db_create)

# %% [markdown]
# #### Dest table

# %%
sql_ddl_drop_table = f"""
    DROP TABLE IF EXISTS {dest_final_table}
"""

# %%
sql_ddl_create_table = f"""
        create table if not exists {dest_final_table}
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
        using iceberg
        """        

# %% [markdown]
# #### SQL DDL Execution

# %%
## drop table
spark.sql(sql_ddl_drop_table)

## create table
spark.sql(sql_ddl_create_table)

# %% [markdown]
# ### Small transformation

# %%
# # some transformations
#     df = df.withColumn("duration", df["duration"].cast("double"))
#     # df = df.withColumn("event_date", to_date(df["record_opening_time"], "yyyyMMddHHmmss"))
#     # to_date(df["record_opening_time"], "yyyyMMddHHmmss")

#     df.withColumn("event_date", from_unixtime(unix_timestamp("record_opening_time", "yyyyMMddHHmmss")))

#     df.select('event_date').show()

# %% [markdown]
# ### Write table

# %%
# wrintint the data on lakehouse
df_source_data.writeTo(f'{dest_final_table}').append()

# %%
table = spark.table(f'{dest_final_table}')
print(table.printSchema())
print(f"No of Records: {table.count()}")


