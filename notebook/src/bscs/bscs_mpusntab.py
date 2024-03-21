
# ### Libraries and session


import pyspark
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DateType, DecimalType
from pyspark.sql.functions import year, to_date, month, dayofmonth,  from_unixtime, unix_timestamp
from pyspark.sql.functions import current_timestamp

REC_CREATED_COLUMN_NAME = "rec_created"
REC_UPDATED_COLUMN_NAME = "rec_updated"

# ip and environments
environment = 'prd'

# Source
system_source = "bscs"
system_table ="bscs_mpusntab"

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


spark = SparkSession.builder\
    .appName(appname)\
    .getOrCreate()


print("=================================================")
spark.sparkContext.setLogLevel(log_level)
print(pyspark.SparkConf().getAll())


# ### functions

def audit_add_column(df):
    """
    Add audit columns with current date to the DataFrame.
    
    Args:
        df (DataFrame): The DataFrame to which the audit columns are added.
    
    Returns:
        DataFrame: DataFrame with the audit columns added.
    """
    created_column_name = REC_CREATED_COLUMN_NAME
    updated_column_name = REC_UPDATED_COLUMN_NAME
    # Add audit column with current date
    df_with_audit = df.withColumn(created_column_name, current_timestamp()) \
                      .withColumn(updated_column_name, current_timestamp())
    
    return df_with_audit
# ### Read from the source


s3 = boto3.client('s3', endpoint_url=s3_endpoint, aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key)


# List all files in the source directory
file_list = []
paginator = s3.get_paginator('list_objects_v2')

for result in paginator.paginate(Bucket=f"{environment}-{source_bucket}", Prefix=source_folder):
    
    if 'Contents' in result:
        for item in result['Contents']:
            file_list.append(item['Key'])



# #### Data Contract


df_source_schema = StructType([
                        StructField("sncode", StringType()),
                        StructField("des", StringType()),
                        StructField("shdes", StringType()),
                        StructField("snind", StringType()),
                        StructField("rec_version", StringType()),
                        StructField("dwh_etl_history_fk", StringType()),
                        StructField("flg_processed", StringType()),
                        StructField("flg_error", StringType()),
                        StructField("error_desc", StringType()),
                        StructField("stg_record_load_date", StringType())
                ])


num_columns_contract = len(df_source_schema.fields)
print("Number of columns of contract:", num_columns_contract)


df_source_data = spark.createDataFrame([], schema=df_source_schema)


# reading files in the source
for file_name in file_list:

    print(f'File in processing: {file_name}')
    
    df = spark.read.format("csv") \
                    .option("header", "true") \
                    .option("delimiter", ",") \
                    .schema(df_source_schema) \
                    .load(f"s3a://{environment}-{source_bucket}/{file_name}")
    

    
    if len(df.columns) == num_columns_contract:
        # print('No of columns matched')
        df_source_data = df_source_data.union(df)


### Small transformation
df_send_to_bronze = audit_add_column(df_source_data)

# ### DDL on lakehouse
#### Data base
##creating db
sql_db_create = f"""
CREATE DATABASE IF NOT EXISTS {dest_final_db} COMMENT '' LOCATION 's3a://{environment}-{lakehouse_bucket}/{dest_db_catalog}/{dest_db_schema}/'
"""
print(sql_db_create)
spark.sql(sql_db_create)


# #### Dest table


sql_ddl_drop_table = f"""
    DROP TABLE IF EXISTS {dest_final_table}
"""


sql_ddl_create_table = f"""
        create table if not exists {dest_final_table}
        (
			sncode string,
			des string,
			shdes string,
			snind string,
			rec_version string,
			dwh_etl_history_fk string,
			flg_processed string,
			flg_error string,
			error_desc string,
			stg_record_load_date string,
            rec_created timestamp,
            rec_updated timestamp
        ) 
        using iceberg
        """


# #### SQL DDL Execution
## drop table
spark.sql(sql_ddl_drop_table)

## create table
spark.sql(sql_ddl_create_table)

# ### Write table
# wrintint the data on lakehouse
df_send_to_bronze.writeTo(f'{dest_final_table}').append()


table = spark.table(f'{dest_final_table}')
print(table.printSchema())
print(f"No of Records: {table.count()}")


