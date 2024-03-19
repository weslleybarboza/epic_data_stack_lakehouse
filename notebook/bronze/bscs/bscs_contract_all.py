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
system_table ="bscs_contract_all"

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
        StructField("customer_id", StringType()),
        StructField("type", StringType()),
        StructField("ohxact", StringType()),
        StructField("hrcode", StringType()),
        StructField("plcode", StringType()),
        StructField("sccode", StringType()),
        StructField("subm_id", StringType()),
        StructField("co_signed", StringType()),
        StructField("co_cancelled", StringType()),
        StructField("co_suspended", StringType()),
        StructField("co_reactivated", StringType()),
        StructField("co_status", StringType()),
        StructField("co_status_date", StringType()),
        StructField("co_requstatus", StringType()),
        StructField("co_requstatus_date", StringType()),
        StructField("co_rs_id", StringType()),
        StructField("co_equ_type", StringType()),
        StructField("co_rep_bill", StringType()),
        StructField("co_rep", StringType()),
        StructField("co_rep_bill_idno", StringType()),
        StructField("co_rep_idno", StringType()),
        StructField("co_installed", StringType()),
        StructField("co_itemized_bill", StringType()),
        StructField("co_ib_categories", StringType()),
        StructField("co_ib_threshold", StringType()),
        StructField("co_archive", StringType()),
        StructField("co_dir_entry", StringType()),
        StructField("co_operator_dir", StringType()),
        StructField("co_pstn_dir", StringType()),
        StructField("co_calls_anonym", StringType()),
        StructField("co_ass_serv", StringType()),
        StructField("co_ass_equ", StringType()),
        StructField("co_ass_cbb", StringType()),
        StructField("co_crd_check", StringType()),
        StructField("co_crd_chk_end", StringType()),
        StructField("co_crd_chk_start", StringType()),
        StructField("co_crd_clicks", StringType()),
        StructField("co_crd_clicks_day", StringType()),
        StructField("co_crd_days", StringType()),
        StructField("co_comment", StringType()),
        StructField("co_duration", StringType()),
        StructField("co_reserved", StringType()),
        StructField("co_expir_date", StringType()),
        StructField("co_activated", StringType()),
        StructField("co_entdate", StringType()),
        StructField("co_moddate", StringType()),
        StructField("co_userlastmod", StringType()),
        StructField("rec_version", StringType()),
        StructField("co_tollrating", StringType()),
        StructField("tmcode", StringType()),
        StructField("tmcode_date", StringType()),
        StructField("co_crd_d_tr1", StringType()),
        StructField("co_crd_d_tr2", StringType()),
        StructField("co_crd_d_tr3", StringType()),
        StructField("co_crd_p_tr1", StringType()),
        StructField("co_crd_p_tr2", StringType()),
        StructField("co_crd_p_tr3", StringType()),
        StructField("ixcode", StringType()),
        StructField("pending_ixcode", StringType()),
        StructField("co_request", StringType()),
        StructField("co_multinumbering", StringType()),
        StructField("eccode_ldc", StringType()),
        StructField("pending_eccode_ldc", StringType()),
        StructField("eccode_lec", StringType()),
        StructField("pending_eccode_lec", StringType()),
        StructField("dealer_id", StringType()),
        StructField("svp_contract", StringType()),
        StructField("not_valid", StringType()),
        StructField("arpcode", StringType()),
        StructField("contr_curr_id", StringType()),
        StructField("convratetype_contract", StringType()),
        StructField("co_addr_on_ibill", StringType()),
        StructField("co_crd_amount", StringType()),
        StructField("co_crd_amount_day", StringType()),
        StructField("product_history_date", StringType()),
        StructField("co_confirm", StringType()),
        StructField("co_timm_modified", StringType()),
        StructField("co_ib_mass_flag", StringType()),
        StructField("trial_end_date", StringType()),
        StructField("co_ext_csuin", StringType()),
        StructField("co_ib_cdr_flag", StringType()),
        StructField("currency", StringType()),
        StructField("sec_contr_curr_id", StringType()),
        StructField("sec_convratetype_contract", StringType()),
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
df_source_data.show(100)

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
			customer_id string,
			type string,
			ohxact string,
			hrcode string,
			plcode string,
			sccode string,
			subm_id string,
			co_signed string,
			co_cancelled string,
			co_suspended string,
			co_reactivated string,
			co_status string,
			co_status_date string,
			co_requstatus string,
			co_requstatus_date string,
			co_rs_id string,
			co_equ_type string,
			co_rep_bill string,
			co_rep string,
			co_rep_bill_idno string,
			co_rep_idno string,
			co_installed string,
			co_itemized_bill string,
			co_ib_categories string,
			co_ib_threshold string,
			co_archive string,
			co_dir_entry string,
			co_operator_dir string,
			co_pstn_dir string,
			co_calls_anonym string,
			co_ass_serv string,
			co_ass_equ string,
			co_ass_cbb string,
			co_crd_check string,
			co_crd_chk_end string,
			co_crd_chk_start string,
			co_crd_clicks string,
			co_crd_clicks_day string,
			co_crd_days string,
			co_comment string,
			co_duration string,
			co_reserved string,
			co_expir_date string,
			co_activated string,
			co_entdate string,
			co_moddate string,
			co_userlastmod string,
			rec_version string,
			co_tollrating string,
			tmcode string,
			tmcode_date string,
			co_crd_d_tr1 string,
			co_crd_d_tr2 string,
			co_crd_d_tr3 string,
			co_crd_p_tr1 string,
			co_crd_p_tr2 string,
			co_crd_p_tr3 string,
			ixcode string,
			pending_ixcode string,
			co_request string,
			co_multinumbering string,
			eccode_ldc string,
			pending_eccode_ldc string,
			eccode_lec string,
			pending_eccode_lec string,
			dealer_id string,
			svp_contract string,
			not_valid string,
			arpcode string,
			contr_curr_id string,
			convratetype_contract string,
			co_addr_on_ibill string,
			co_crd_amount string,
			co_crd_amount_day string,
			product_history_date string,
			co_confirm string,
			co_timm_modified string,
			co_ib_mass_flag string,
			trial_end_date string,
			co_ext_csuin string,
			co_ib_cdr_flag string,
			currency string,
			sec_contr_curr_id string,
			sec_convratetype_contract string,
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


