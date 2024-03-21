from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
# from pyspark.sql.types import StructType, StringType

REC_CREATED_COLUMN_NAME = "rec_created"
REC_UPDATED_COLUMN_NAME = "rec_updated"

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

def audit_update_column(df):
    """
    Update the specified audit column with the current timestamp.
    
    Args:
        df (DataFrame): The DataFrame to which the audit column is updated.
    
    Returns:
        DataFrame: DataFrame with the specified audit column updated.
    """
    update_column_name = REC_UPDATED_COLUMN_NAME

    # Update the specified audit column with current timestamp
    df_updated = df.withColumn(update_column_name, current_timestamp())
    
    return df_updated


# Example usage
# if __name__ == "__main__":
#     # Create a SparkSession
#     spark = SparkSession.builder \
#         .appName("AddAuditColumn") \
#         .getOrCreate()
    
#     # Define schema for DataFrame
#     schema = StructType().add("name", StringType()).add("age", StringType())
    
#     # Create a sample DataFrame
#     data = [("Alice", "30"), ("Bob", "35")]
#     df = spark.createDataFrame(data, schema)
    
#     # Add audit column to DataFrame
#     df_with_audit = add_audit_column(df)
    

#     # Show DataFrame with audit column
#     df_with_audit.show(truncate=False)

#     df_upd = update_audit_column(df_with_audit)

#     df_upd.show(truncate=False)