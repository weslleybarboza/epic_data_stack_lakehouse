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
system_table ="bscs_customer_all"

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
                    StructField("customer_id", IntegerType()),
                    StructField("customer_id_high", IntegerType()),
                    StructField("custcode", StringType()),
                    StructField("csst", StringType()),
                    StructField("cstype", StringType()),
                    StructField("csactivated", DateType()),
                    StructField("csdeactivated", DateType()),
                    StructField("customer_dealer", StringType()),
                    StructField("cstype_date", DateType()),
                    StructField("cstaxable", StringType()),
                    StructField("cslevel", StringType()),
                    StructField("cscusttype", StringType()),
                    StructField("cslvlname", StringType()),
                    StructField("cpcode", DecimalType()),
                    StructField("tmcode", DecimalType()),
                    StructField("prgcode", StringType()),
                    StructField("termcode", DecimalType()),
                    StructField("csclimit", DecimalType()),
                    StructField("cscurbalance", DecimalType()),
                    StructField("csdepdate", DateType()),
                    StructField("billcycle", StringType()),
                    StructField("nobillprint", StringType()),
                    StructField("nobillstart", DateType()),
                    StructField("nobillstop", DateType()),
                    StructField("cstestbillrun", StringType()),
                    StructField("bill_layout", DecimalType()),
                    StructField("paymntresp", StringType()),
                    StructField("summsheet", StringType()),
                    StructField("csmethpaymnt", DecimalType()),
                    StructField("payresp_groups", DecimalType()),
                    StructField("target_reached", DecimalType()),
                    StructField("pcsmethpaymnt", StringType()),
                    StructField("passportno", StringType()),
                    StructField("birthdate", DateType()),
                    StructField("marketing_flag", StringType()),
                    StructField("dunning_flag", StringType()),
                    StructField("comm_no", StringType()),
                    StructField("pos_comm_type", DecimalType()),
                    StructField("btx_password", StringType()),
                    StructField("btx_user", StringType()),
                    StructField("settles_p_month", StringType()),
                    StructField("cashretour", DecimalType()),
                    StructField("cstradecode", StringType()),
                    StructField("cspassword", StringType()),
                    StructField("csaddryears", DecimalType()),
                    StructField("cspromotion", StringType()),
                    StructField("cscompregno", StringType()),
                    StructField("cscomptaxno", StringType()),
                    StructField("csreason", DecimalType()),
                    StructField("cscollector", StringType()),
                    StructField("cscontresp", StringType()),
                    StructField("csdeposit", DecimalType()),
                    StructField("suspended", DateType()),
                    StructField("reactivated", DateType()),
                    StructField("bundling", StringType()),
                    StructField("prev_balance", DecimalType()),
                    StructField("lbc_date", DateType()),
                    StructField("employee", StringType()),
                    StructField("company_type", StringType()),
                    StructField("crlimit_exc", StringType()),
                    StructField("area_id", DecimalType()),
                    StructField("costcenter_id", DecimalType()),
                    StructField("csfedtaxid", StringType()),
                    StructField("credit_rating", DecimalType()),
                    StructField("cscredit_status", StringType()),
                    StructField("deact_create_date", DateType()),
                    StructField("deact_receip_date", DateType()),
                    StructField("cscrdinreasn", StringType()),
                    StructField("cscrdindate", DateType()),
                    StructField("cscrdinclaim", DecimalType()),
                    StructField("cscrdinlimit", DecimalType()),
                    StructField("cscrdinstatus", StringType()),
                    StructField("cscrdinlast_act", DateType()),
                    StructField("edifact_addr", StringType()),
                    StructField("edifact_user_flag", StringType()),
                    StructField("edifact_flag", StringType()),
                    StructField("csdeposit_due_date", DateType()),
                    StructField("calculate_deposit", StringType()),
                    StructField("tmcode_date", DateType()),
                    StructField("cslanguage", DecimalType()),
                    StructField("csrentalbc", StringType()),
                    StructField("id_type", DecimalType()),
                    StructField("user_lastmod", StringType()),
                    StructField("csentdate", DateType()),
                    StructField("csmoddate", DateType()),
                    StructField("csmod", StringType()),
                    StructField("csnationality", DecimalType()),
                    StructField("csbillmedium", DecimalType()),
                    StructField("csitembillmedium", DecimalType()),
                    StructField("rec_version", DecimalType()),
                    StructField("dunn_date_1", DateType()),
                    StructField("dunn_date_2", DateType()),
                    StructField("dunn_date_3", DateType()),
                    StructField("dunn_date_4", DateType()),
                    StructField("dunn_date_5", DateType()),
                    StructField("dunn_date_6", DateType()),
                    StructField("cscredit_date", DateType()),
                    StructField("cscredit_remark", StringType()),
                    StructField("customer_id_ext", StringType()),
                    StructField("cslimit_o_tr1", DecimalType()),
                    StructField("cslimit_o_tr2", DecimalType()),
                    StructField("cslimit_o_tr3", DecimalType()),
                    StructField("lbc_date_hist", StringType()),
                    StructField("cscredit_score", StringType()),
                    StructField("cstraderef", StringType()),
                    StructField("cssocialsecno", StringType()),
                    StructField("csdrivelicence", StringType()),
                    StructField("cssex", StringType()),
                    StructField("csemployer", StringType()),
                    StructField("cstaxable_reason", StringType()),
                    StructField("dmcode_subs", DecimalType()),
                    StructField("dmcode_access", DecimalType()),
                    StructField("dmcode_usage", DecimalType()),
                    StructField("csreseller", StringType()),
                    StructField("csclimit_o_tr1", DecimalType()),
                    StructField("csclimit_o_tr2", DecimalType()),
                    StructField("csclimit_o_tr3", DecimalType()),
                    StructField("wpid", DecimalType()),
                    StructField("csprepayment", StringType()),
                    StructField("cssumaddr", StringType()),
                    StructField("dmcode_contract", DecimalType()),
                    StructField("csremark_1", StringType()),
                    StructField("csremark_2", StringType()),
                    StructField("ma_id", DecimalType()),
                    StructField("dunning_status", StringType()),
                    StructField("dunn_date_7", DateType()),
                    StructField("dunn_date_8", DateType()),
                    StructField("dunn_date_9", DateType()),
                    StructField("dunn_date_10", DateType()),
                    StructField("bill_information", StringType()),
                    StructField("dealer_id", DecimalType()),
                    StructField("not_valid", StringType()),
                    StructField("dunning_mode", StringType()),
                    StructField("cscrdcheck_agreed", StringType()),
                    StructField("marital_status", DecimalType()),
                    StructField("expect_pay_curr_id", DecimalType()),
                    StructField("convratetype_payment", DecimalType()),
                    StructField("refund_curr_id", DecimalType()),
                    StructField("convratetype_refund", DecimalType()),
                    StructField("srcode", DecimalType()),
                    StructField("currency", DecimalType()),
                    StructField("primary_doc_currency", DecimalType()),
                    StructField("secondary_doc_currency", DecimalType()),
                    StructField("prim_convratetype_doc", DecimalType()),
                    StructField("sec_convratetype_doc", DecimalType()),
                    StructField("dwh_etl_history_fk", DecimalType()),
                    StructField("flg_processed", StringType()),
                    StructField("flg_error", StringType()),
                    StructField("error_desc", StringType()),
                    StructField("stg_record_load_date", DateType())
                ])

# %%
df_source_schema_1 = StructType([
                    StructField("customer_id", StringType()),
                    StructField("customer_id_high", StringType()),
                    StructField("custcode", StringType()),
                    StructField("csst", StringType()),
                    StructField("cstype", StringType()),
                    StructField("csactivated", StringType()),
                    StructField("csdeactivated", StringType()),
                    StructField("customer_dealer", StringType()),
                    StructField("cstype_date", StringType()),
                    StructField("cstaxable", StringType()),
                    StructField("cslevel", StringType()),
                    StructField("cscusttype", StringType()),
                    StructField("cslvlname", StringType()),
                    StructField("cpcode", StringType()),
                    StructField("tmcode", StringType()),
                    StructField("prgcode", StringType()),
                    StructField("termcode", StringType()),
                    StructField("csclimit", StringType()),
                    StructField("cscurbalance", StringType()),
                    StructField("csdepdate", StringType()),
                    StructField("billcycle", StringType()),
                    StructField("nobillprint", StringType()),
                    StructField("nobillstart", StringType()),
                    StructField("nobillstop", StringType()),
                    StructField("cstestbillrun", StringType()),
                    StructField("bill_layout", StringType()),
                    StructField("paymntresp", StringType()),
                    StructField("summsheet", StringType()),
                    StructField("csmethpaymnt", StringType()),
                    StructField("payresp_groups", StringType()),
                    StructField("target_reached", StringType()),
                    StructField("pcsmethpaymnt", StringType()),
                    StructField("passportno", StringType()),
                    StructField("birthdate", StringType()),
                    StructField("marketing_flag", StringType()),
                    StructField("dunning_flag", StringType()),
                    StructField("comm_no", StringType()),
                    StructField("pos_comm_type", StringType()),
                    StructField("btx_password", StringType()),
                    StructField("btx_user", StringType()),
                    StructField("settles_p_month", StringType()),
                    StructField("cashretour", StringType()),
                    StructField("cstradecode", StringType()),
                    StructField("cspassword", StringType()),
                    StructField("csaddryears", StringType()),
                    StructField("cspromotion", StringType()),
                    StructField("cscompregno", StringType()),
                    StructField("cscomptaxno", StringType()),
                    StructField("csreason", StringType()),
                    StructField("cscollector", StringType()),
                    StructField("cscontresp", StringType()),
                    StructField("csdeposit", StringType()),
                    StructField("suspended", StringType()),
                    StructField("reactivated", StringType()),
                    StructField("bundling", StringType()),
                    StructField("prev_balance", StringType()),
                    StructField("lbc_date", StringType()),
                    StructField("employee", StringType()),
                    StructField("company_type", StringType()),
                    StructField("crlimit_exc", StringType()),
                    StructField("area_id", StringType()),
                    StructField("costcenter_id", StringType()),
                    StructField("csfedtaxid", StringType()),
                    StructField("credit_rating", StringType()),
                    StructField("cscredit_status", StringType()),
                    StructField("deact_create_date", StringType()),
                    StructField("deact_receip_date", StringType()),
                    StructField("cscrdinreasn", StringType()),
                    StructField("cscrdindate", StringType()),
                    StructField("cscrdinclaim", StringType()),
                    StructField("cscrdinlimit", StringType()),
                    StructField("cscrdinstatus", StringType()),
                    StructField("cscrdinlast_act", StringType()),
                    StructField("edifact_addr", StringType()),
                    StructField("edifact_user_flag", StringType()),
                    StructField("edifact_flag", StringType()),
                    StructField("csdeposit_due_date", StringType()),
                    StructField("calculate_deposit", StringType()),
                    StructField("tmcode_date", StringType()),
                    StructField("cslanguage", StringType()),
                    StructField("csrentalbc", StringType()),
                    StructField("id_type", StringType()),
                    StructField("user_lastmod", StringType()),
                    StructField("csentdate", StringType()),
                    StructField("csmoddate", StringType()),
                    StructField("csmod", StringType()),
                    StructField("csnationality", StringType()),
                    StructField("csbillmedium", StringType()),
                    StructField("csitembillmedium", StringType()),
                    StructField("rec_version", StringType()),
                    StructField("dunn_date_1", StringType()),
                    StructField("dunn_date_2", StringType()),
                    StructField("dunn_date_3", StringType()),
                    StructField("dunn_date_4", StringType()),
                    StructField("dunn_date_5", StringType()),
                    StructField("dunn_date_6", StringType()),
                    StructField("cscredit_date", StringType()),
                    StructField("cscredit_remark", StringType()),
                    StructField("customer_id_ext", StringType()),
                    StructField("cslimit_o_tr1", StringType()),
                    StructField("cslimit_o_tr2", StringType()),
                    StructField("cslimit_o_tr3", StringType()),
                    StructField("lbc_date_hist", StringType()),
                    StructField("cscredit_score", StringType()),
                    StructField("cstraderef", StringType()),
                    StructField("cssocialsecno", StringType()),
                    StructField("csdrivelicence", StringType()),
                    StructField("cssex", StringType()),
                    StructField("csemployer", StringType()),
                    StructField("cstaxable_reason", StringType()),
                    StructField("dmcode_subs", StringType()),
                    StructField("dmcode_access", StringType()),
                    StructField("dmcode_usage", StringType()),
                    StructField("csreseller", StringType()),
                    StructField("csclimit_o_tr1", StringType()),
                    StructField("csclimit_o_tr2", StringType()),
                    StructField("csclimit_o_tr3", StringType()),
                    StructField("wpid", StringType()),
                    StructField("csprepayment", StringType()),
                    StructField("cssumaddr", StringType()),
                    StructField("dmcode_contract", StringType()),
                    StructField("csremark_1", StringType()),
                    StructField("csremark_2", StringType()),
                    StructField("ma_id", StringType()),
                    StructField("dunning_status", StringType()),
                    StructField("dunn_date_7", StringType()),
                    StructField("dunn_date_8", StringType()),
                    StructField("dunn_date_9", StringType()),
                    StructField("dunn_date_10", StringType()),
                    StructField("bill_information", StringType()),
                    StructField("dealer_id", StringType()),
                    StructField("not_valid", StringType()),
                    StructField("dunning_mode", StringType()),
                    StructField("cscrdcheck_agreed", StringType()),
                    StructField("marital_status", StringType()),
                    StructField("expect_pay_curr_id", StringType()),
                    StructField("convratetype_payment", StringType()),
                    StructField("refund_curr_id", StringType()),
                    StructField("convratetype_refund", StringType()),
                    StructField("srcode", StringType()),
                    StructField("currency", StringType()),
                    StructField("primary_doc_currency", StringType()),
                    StructField("secondary_doc_currency", StringType()),
                    StructField("prim_convratetype_doc", StringType()),
                    StructField("sec_convratetype_doc", StringType()),
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
    
    df.show(5)
    
    if len(df.columns) == num_columns_contract:
        print('No of columns matched')
        df_source_data = df_source_data.union(df)

# %%
# print("No of lines to load: ", len(df_source_data))
df_source_data.show(10)

# %%
df_source_data.describe()

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
sql_ddl_create_table_1 = f"""
        create table if not exists {dest_final_table}
        (
        	customer_id int,
        	customer_id_high int,
        	custcode string,
        	csst string,
        	cstype string,
        	csactivated date,
        	csdeactivated date,
        	customer_dealer string,
        	cstype_date date,
        	cstaxable string,
        	cslevel string,
        	cscusttype string,
        	cslvlname string,
        	cpcode decimal(38,0),
        	tmcode decimal(38,0),
        	prgcode string,
        	termcode decimal(4,0),
        	csclimit decimal,
        	cscurbalance decimal,
        	csdepdate date,
        	billcycle string,
        	nobillprint string,
        	nobillstart date,
        	nobillstop date,
        	cstestbillrun string,
        	bill_layout decimal(38,0),
        	paymntresp string,
        	summsheet string,
        	csmethpaymnt decimal(38,0),
        	payresp_groups decimal(38,0),
        	target_reached decimal(38,0),
        	pcsmethpaymnt string,
        	passportno string,
        	birthdate date,
        	marketing_flag string,
        	dunning_flag string,
        	comm_no string,
        	pos_comm_type decimal(38,0),
        	btx_password string,
        	btx_user string,
        	settles_p_month string,
        	cashretour decimal(38,0),
        	cstradecode string,
        	cspassword string,
        	csaddryears decimal(38,0),
        	cspromotion string,
        	cscompregno string,
        	cscomptaxno string,
        	csreason decimal(38,0),
        	cscollector string,
        	cscontresp string,
        	csdeposit decimal,
        	suspended date,
        	reactivated date,
        	bundling string,
        	prev_balance decimal,
        	lbc_date date,
        	employee string,
        	company_type string,
        	crlimit_exc string,
        	area_id decimal(38,0),
        	costcenter_id decimal(38,0),
        	csfedtaxid string,
        	credit_rating decimal(38,0),
        	cscredit_status string,
        	deact_create_date date,
        	deact_receip_date date,
        	cscrdinreasn string,
        	cscrdindate date,
        	cscrdinclaim decimal,
        	cscrdinlimit decimal,
        	cscrdinstatus string,
        	cscrdinlast_act date,
        	edifact_addr string,
        	edifact_user_flag string,
        	edifact_flag string,
        	csdeposit_due_date date,
        	calculate_deposit string,
        	tmcode_date date,
        	cslanguage decimal(38,0),
        	csrentalbc string,
        	id_type decimal(38,0),
        	user_lastmod string,
        	csentdate date,
        	csmoddate date,
        	csmod string,
        	csnationality decimal(38,0),
        	csbillmedium decimal(38,0),
        	csitembillmedium decimal(38,0),
        	rec_version decimal(38,0),
        	dunn_date_1 date,
        	dunn_date_2 date,
        	dunn_date_3 date,
        	dunn_date_4 date,
        	dunn_date_5 date,
        	dunn_date_6 date,
        	cscredit_date date,
        	cscredit_remark string,
        	customer_id_ext string,
        	cslimit_o_tr1 decimal(38,0),
        	cslimit_o_tr2 decimal(38,0),
        	cslimit_o_tr3 decimal(38,0),
        	lbc_date_hist string,
        	cscredit_score string,
        	cstraderef string,
        	cssocialsecno string,
        	csdrivelicence string,
        	cssex string,
        	csemployer string,
        	cstaxable_reason string,
        	dmcode_subs decimal(38,0),
        	dmcode_access decimal(38,0),
        	dmcode_usage decimal(38,0),
        	csreseller string,
        	csclimit_o_tr1 decimal(38,0),
        	csclimit_o_tr2 decimal(38,0),
        	csclimit_o_tr3 decimal(38,0),
        	wpid decimal(38,0),
        	csprepayment string,
        	cssumaddr string,
        	dmcode_contract decimal,
        	csremark_1 string,
        	csremark_2 string,
        	ma_id decimal,
        	dunning_status string,
        	dunn_date_7 date,
        	dunn_date_8 date,
        	dunn_date_9 date,
        	dunn_date_10 date,
        	bill_information string,
        	dealer_id decimal(38,0),
        	not_valid string,
        	dunning_mode string,
        	cscrdcheck_agreed string,
        	marital_status decimal(38,0),
        	expect_pay_curr_id decimal(38,0),
        	convratetype_payment decimal(38,0),
        	refund_curr_id decimal(38,0),
        	convratetype_refund decimal(38,0),
        	srcode decimal(38,0),
        	currency decimal(38,0),
        	primary_doc_currency decimal(38,0),
        	secondary_doc_currency decimal(38,0),
        	prim_convratetype_doc decimal(38,0),
        	sec_convratetype_doc decimal(38,0),
        	dwh_etl_history_fk decimal(38,0),
        	flg_processed char(1),
        	flg_error char(1),
        	error_desc char(250),
        	stg_record_load_date date
        ) 
        using iceberg
        """        

# %%
sql_ddl_create_table = f"""
        create table if not exists {dest_final_table}
        (
            customer_id string,
            customer_id_high string,
            custcode string,
            csst string,
            cstype string,
            csactivated string,
            csdeactivated string,
            customer_dealer string,
            cstype_date string,
            cstaxable string,
            cslevel string,
            cscusttype string,
            cslvlname string,
            cpcode string,
            tmcode string,
            prgcode string,
            termcode string,
            csclimit string,
            cscurbalance string,
            csdepdate string,
            billcycle string,
            nobillprint string,
            nobillstart string,
            nobillstop string,
            cstestbillrun string,
            bill_layout string,
            paymntresp string,
            summsheet string,
            csmethpaymnt string,
            payresp_groups string,
            target_reached string,
            pcsmethpaymnt string,
            passportno string,
            birthdate string,
            marketing_flag string,
            dunning_flag string,
            comm_no string,
            pos_comm_type string,
            btx_password string,
            btx_user string,
            settles_p_month string,
            cashretour string,
            cstradecode string,
            cspassword string,
            csaddryears string,
            cspromotion string,
            cscompregno string,
            cscomptaxno string,
            csreason string,
            cscollector string,
            cscontresp string,
            csdeposit string,
            suspended string,
            reactivated string,
            bundling string,
            prev_balance string,
            lbc_date string,
            employee string,
            company_type string,
            crlimit_exc string,
            area_id string,
            costcenter_id string,
            csfedtaxid string,
            credit_rating string,
            cscredit_status string,
            deact_create_date string,
            deact_receip_date string,
            cscrdinreasn string,
            cscrdindate string,
            cscrdinclaim string,
            cscrdinlimit string,
            cscrdinstatus string,
            cscrdinlast_act string,
            edifact_addr string,
            edifact_user_flag string,
            edifact_flag string,
            csdeposit_due_date string,
            calculate_deposit string,
            tmcode_date string,
            cslanguage string,
            csrentalbc string,
            id_type string,
            user_lastmod string,
            csentdate string,
            csmoddate string,
            csmod string,
            csnationality string,
            csbillmedium string,
            csitembillmedium string,
            rec_version string,
            dunn_date_1 string,
            dunn_date_2 string,
            dunn_date_3 string,
            dunn_date_4 string,
            dunn_date_5 string,
            dunn_date_6 string,
            cscredit_date string,
            cscredit_remark string,
            customer_id_ext string,
            cslimit_o_tr1 string,
            cslimit_o_tr2 string,
            cslimit_o_tr3 string,
            lbc_date_hist string,
            cscredit_score string,
            cstraderef string,
            cssocialsecno string,
            csdrivelicence string,
            cssex string,
            csemployer string,
            cstaxable_reason string,
            dmcode_subs string,
            dmcode_access string,
            dmcode_usage string,
            csreseller string,
            csclimit_o_tr1 string,
            csclimit_o_tr2 string,
            csclimit_o_tr3 string,
            wpid string,
            csprepayment string,
            cssumaddr string,
            dmcode_contract string,
            csremark_1 string,
            csremark_2 string,
            ma_id string,
            dunning_status string,
            dunn_date_7 string,
            dunn_date_8 string,
            dunn_date_9 string,
            dunn_date_10 string,
            bill_information string,
            dealer_id string,
            not_valid string,
            dunning_mode string,
            cscrdcheck_agreed string,
            marital_status string,
            expect_pay_curr_id string,
            convratetype_payment string,
            refund_curr_id string,
            convratetype_refund string,
            srcode string,
            currency string,
            primary_doc_currency string,
            secondary_doc_currency string,
            prim_convratetype_doc string,
            sec_convratetype_doc string,
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


