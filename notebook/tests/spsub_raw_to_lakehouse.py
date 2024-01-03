import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import year, to_date, month, dayofmonth

# ip and environments
environment = 'dev'

# Set the bucket and folder paths
source_bucket = 'bronze'
source_folder = 'files/pscore/ascll/20231218'

lakehouse_bucket = 'bronze'
lakehouse_folder = 'datalake/iceberg'

# table destination settings
dest_db_catalog = 'iceberg'
dest_db_schema = 'pscore'
dest_db_table = 'sgw'
dest_final_db = f'{dest_db_catalog}.{dest_db_schema}'
dest_final_table = f'{dest_final_db}.{dest_db_table}'

# Spark identification and settings
appname = 'SGW_from_raw_to_raw_refined'
log_level = 'WARN' # Valid log levels include: ALL, DEBUG, ERROR, FATAL, INFO, OFF, TRACE, WARN

spark = SparkSession.builder\
    .appName(appname)\
    .getOrCreate()


spark.sparkContext.setLogLevel(log_level)
print(pyspark.SparkConf().getAll())

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
                    StructField("year", StringType()),
                    StructField("month", StringType()),
                    StructField("day", StringType())
                ])

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS iceberg.raw COMMENT '' LOCATION 's3a://datalake/iceberg/raw/'
""")

table = spark.sql(f"""
        CREATE TABLE IF NOT EXISTS iceberg.raw.sgw
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
            year STRING,
            month STRING,
            day STRING
        ) 
        USING iceberg
        PARTITIONED BY (year, month, day)
        """)
        
# reading files in the source
df = spark.read.format("csv") \
                .option("header", "false") \
                .option("delimiter", ";") \
                .schema(schema) \
                .load(f"s3a://{environment}-{source_bucket}/{source_folder}/*.cdr")

# some transformations
df = df.withColumn("duration", df["duration"].cast("double"))
df = df.withColumn("year", year(to_date(df["record_opening_time"], "yyyyMMddHHmmss")))
df = df.withColumn("month", month(to_date(df["record_opening_time"], "yyyyMMddHHmmss")))
df = df.withColumn("day", dayofmonth(to_date(df["record_opening_time"], "yyyyMMddHHmmss")))

df.writeTo('iceberg.raw.sgw').append() 

tb = spark.table('iceberg.raw.sgw')
print(tb.printSchema())
print(f"No of Records: {tb.count()}")