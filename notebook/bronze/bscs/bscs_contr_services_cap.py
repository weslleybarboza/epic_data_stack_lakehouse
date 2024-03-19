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
system_source = "bscs"
system_table ="bscs_contr_services_cap"

# Set the bucket and folder paths
source_bucket = 'landing-zone'
source_folder = f'database/{system_source}/{system_table}'

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
                        StructField("co_id", StringType()),
                        StructField("sncode", StringType()),
                        StructField("seqno", StringType()),
                        StructField("seqno_pre", StringType()),
                        StructField("bccode", StringType()),
                        StructField("pending_bccode", StringType()),
                        StructField("dn_id", StringType()),
                        StructField("main_dirnum", StringType()),
                        StructField("cs_status", StringType()),
                        StructField("cs_activ_date", StringType()),
                        StructField("cs_deactiv_date", StringType()),
                        StructField("cs_request", StringType()),
                        StructField("rec_version", StringType()),
                        StructField("dn_block_id", StringType()),
                        StructField("dwh_etl_history_fk", StringType()),
                        StructField("flg_processed", StringType()),
                        StructField("flg_error", StringType()),
                        StructField("error_desc", StringType()),
                        StructField("stg_record_load_date", StringType())
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
                    .option("delimiter", ",") \
                    .schema(df_source_schema) \
                    .load(f"s3a://{environment}-{source_bucket}/{file_name}")
    
    # df.show(5)
    
    if len(df.columns) == num_columns_contract:
        # print('No of columns matched')
        df_source_data = df_source_data.union(df)

# %%
# print("No of lines to load: ", len(df_source_data))
# df_source_data.show(10)

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
			co_id string,
			sncode string,
			seqno string,
			seqno_pre string,
			bccode string,
			pending_bccode string,
			dn_id string,
			main_dirnum string,
			cs_status string,
			cs_activ_date string,
			cs_deactiv_date string,
			cs_request string,
			rec_version string,
			dn_block_id string,
			dwh_etl_history_fk string,
			flg_processed string,
			flg_error string,
			error_desc string,
			stg_record_load_date string
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


