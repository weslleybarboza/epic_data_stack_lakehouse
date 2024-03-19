# ### Libraries and session
import pyspark
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DateType, DecimalType

# ip and environments
environment = 'prd'

# Source
system_source = "bscs"
system_table ="bscs_ccontact_all"

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
                    StructField("customer_id", StringType()),
                    StructField("ccseq", StringType()),
                    StructField("cctitle", StringType()),
                    StructField("ccname", StringType()),
                    StructField("ccfname", StringType()),
                    StructField("cclname", StringType()),
                    StructField("ccstreet", StringType()),
                    StructField("ccstreetno", StringType()),
                    StructField("cclnamemc", StringType()),
                    StructField("ccaddr1", StringType()),
                    StructField("ccaddr2", StringType()),
                    StructField("ccaddr3", StringType()),
                    StructField("cccity", StringType()),
                    StructField("cczip", StringType()),
                    StructField("cccountry", StringType()),
                    StructField("cctn", StringType()),
                    StructField("cctn2", StringType()),
                    StructField("ccfax", StringType()),
                    StructField("ccline1", StringType()),
                    StructField("ccline2", StringType()),
                    StructField("ccline3", StringType()),
                    StructField("ccline4", StringType()),
                    StructField("ccline5", StringType()),
                    StructField("ccline6", StringType()),
                    StructField("cctn_area", StringType()),
                    StructField("cctn2_area", StringType()),
                    StructField("ccfax_area", StringType()),
                    StructField("ccjobdesc", StringType()),
                    StructField("ccdeftrk", StringType()),
                    StructField("ccuser", StringType()),
                    StructField("ccbill", StringType()),
                    StructField("ccbilldetails", StringType()),
                    StructField("cccontract", StringType()),
                    StructField("ccship", StringType()),
                    StructField("ccmagazine", StringType()),
                    StructField("ccdirectory", StringType()),
                    StructField("ccforward", StringType()),
                    StructField("ccurgent", StringType()),
                    StructField("country", StringType()),
                    StructField("cclanguage", StringType()),
                    StructField("ccadditional", StringType()),
                    StructField("sort_criteria", StringType()),
                    StructField("ccentdate", StringType()),
                    StructField("ccmoddate", StringType()),
                    StructField("ccmod", StringType()),
                    StructField("rec_version", StringType()),
                    StructField("cczip_addition", StringType()),
                    StructField("cccounty", StringType()),
                    StructField("ccstate", StringType()),
                    StructField("ccgeocode", StringType()),
                    StructField("ccvaliddate", StringType()),
                    StructField("ccbill_previous", StringType()),
                    StructField("welcome_crit", StringType()),
                    StructField("ccmname", StringType()),
                    StructField("ccemail", StringType()),
                    StructField("ccaddryears", StringType()),
                    StructField("ccsmsno", StringType()),
                    StructField("ccinccode", StringType()),
                    StructField("userlastmod", StringType()),
                    StructField("ccbilltemp", StringType()),
                    StructField("ccvalidation", StringType()),
                    StructField("ccuser_inst", StringType()),
                    StructField("cclocation_1", StringType()),
                    StructField("cclocation_2", StringType()),
                    StructField("ccremark", StringType()),
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
    
    df.show(5)
    
    if len(df.columns) == num_columns_contract:
        df_source_data = df_source_data.union(df)

### Small transformation


# # some transformations
#     df = df.withColumn("duration", df["duration"].cast("double"))
#     # df = df.withColumn("event_date", to_date(df["record_opening_time"], "yyyyMMddHHmmss"))
#     # to_date(df["record_opening_time"], "yyyyMMddHHmmss")

#     df.withColumn("event_date", from_unixtime(unix_timestamp("record_opening_time", "yyyyMMddHHmmss")))

#     df.select('event_date').show()




# ### DDL on lakehouse


# #### Data base
##creating db
sql_db_create = f"""
CREATE DATABASE IF NOT EXISTS {dest_final_db} COMMENT '' LOCATION 's3a://{environment}-{lakehouse_bucket}/{dest_db_catalog}/{dest_db_schema}/'
"""
print(sql_db_create)
spark.sql(sql_db_create)


# #### Dest table


sql_ddl_drop_table = f"""
    DROP TABLE IF EXISTS  {dest_final_table}
"""


sql_ddl_create_table = f"""
        create table if not exists {dest_final_table}
        (
			customer_id string,
			ccseq string,
			cctitle string,
			ccname string,
			ccfname string,
			cclname string,
			ccstreet string,
			ccstreetno string,
			cclnamemc string,
			ccaddr1 string,
			ccaddr2 string,
			ccaddr3 string,
			cccity string,
			cczip string,
			cccountry string,
			cctn string,
			cctn2 string,
			ccfax string,
			ccline1 string,
			ccline2 string,
			ccline3 string,
			ccline4 string,
			ccline5 string,
			ccline6 string,
			cctn_area string,
			cctn2_area string,
			ccfax_area string,
			ccjobdesc string,
			ccdeftrk string,
			ccuser string,
			ccbill string,
			ccbilldetails string,
			cccontract string,
			ccship string,
			ccmagazine string,
			ccdirectory string,
			ccforward string,
			ccurgent string,
			country string,
			cclanguage string,
			ccadditional string,
			sort_criteria string,
			ccentdate string,
			ccmoddate string,
			ccmod string,
			rec_version string,
			cczip_addition string,
			cccounty string,
			ccstate string,
			ccgeocode string,
			ccvaliddate string,
			ccbill_previous string,
			welcome_crit string,
			ccmname string,
			ccemail string,
			ccaddryears string,
			ccsmsno string,
			ccinccode string,
			userlastmod string,
			ccbilltemp string,
			ccvalidation string,
			ccuser_inst string,
			cclocation_1 string,
			cclocation_2 string,
			ccremark string,
			dwh_etl_history_fk string,
			flg_processed string,
			flg_error string,
			error_desc string,
			stg_record_load_date string
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
df_source_data.writeTo(f'{dest_final_table}').append()


system_table = spark.table(f'{dest_final_table}')
print(system_table.printSchema())
print(f"No of Records: {system_table.count()}")


