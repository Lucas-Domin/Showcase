import sys
import datetime
import json
import boto3
import logging
import traceback
from io import StringIO
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from py4j.java_gateway import java_import
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col as spark_col

# Create a string buffer to store logs
log_buffer = StringIO()

# Configure logging to write to the string buffer
logging.basicConfig(stream=log_buffer, level=logging.INFO, format='%(asctime)s: %(levelname)s: %(message)s')

def upload_logs_to_s3(bucket, table, buffer):
    s3_client = boto3.client('s3')
    key = f"logs/{table}/glue_job_latest_run.log"
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())

def get_secret_credentials(secret_name: str) -> dict:
    logging.info("Retrieving AWS Secrets Manager credentials.")
    try:
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager')
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        return json.loads(get_secret_value_response['SecretString'])
    except Exception as e:
        logging.error(f"Error in get_secret_credentials: {e}")
        raise

def delete_files(bucket: str, prefix: str) -> None:
    logging.info(f"Deleting files from s3://{bucket}/{prefix}")
    try:
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket)
        bucket.objects.filter(Prefix=prefix).delete()
    except Exception as e:
        logging.error(f"Error in delete_files: {e}")
        raise

# Parse arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME", "table", "bucket", "storedprocedure"])

# Initialize Spark and Glue contexts
logging.info("Initializing Spark and Glue contexts.")
sc = SparkContext()
glueContext = GlueContext(sc)
glue = boto3.client('glue')
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")

# Configure variables
logging.info("Configuring variables.")
connection_creds = get_secret_credentials('sirec_creds')
database_url = f"jdbc:{connection_creds['engine']}://{connection_creds['host']}:{connection_creds['port']}"
user = connection_creds['username']
password = connection_creds['password']
schema = "dbo"
table = args["table"]
bucket = args["bucket"]
stored_procedure_query = args['storedprocedure']
glue_table = f"latest_{table}"
glue_database = "raw-latest-db"

# Java imports for JDBC connection
java_import(sc._gateway.jvm, "java.sql.Connection")
java_import(sc._gateway.jvm, "java.sql.DriverManager")
java_import(sc._gateway.jvm, "java.sql.SQLException")

# Connect to the database
logging.info("Connecting to the database.")
conn = None

try:
    conn = sc._gateway.jvm.DriverManager.getConnection(database_url, user, password)
    stmt = conn.createStatement()

    # Define paths for the output files
    output_latest = f"latest/{table}"
    now = datetime.datetime.now()
    output_snapshot = f"snapshots/{table}/year={now.year}/month={now.month:02d}/day={now.day:02d}/time={now.hour:02d}:{now.minute:02d}:{now.second:02d}"
    
    # Clear existing files if any
    delete_files(bucket, output_latest)

    # Execute stored procedure and fetch results
    logging.info(f"Executing stored procedure: {stored_procedure_query}")
    rs = stmt.executeQuery(stored_procedure_query)

    # Process ResultSet
    metadata = rs.getMetaData()
    columns = [metadata.getColumnName(i + 1) for i in range(metadata.getColumnCount())]
    logging.info(f"Columns: {columns}")

    data = []
    row_count = 0
    while rs.next():
        row = [rs.getString(col) for col in columns]
        data.append(row)
        row_count += 1

    if data and columns:
        # Create DataFrame from data and column names
        df = spark.createDataFrame(data, columns)
        column_names_original = df.columns
        
        # Retrieve table schema from AWS Glue Data Catalog
        response = glue.get_table(DatabaseName=glue_database, Name=glue_table)
        table_schema = response['Table']['StorageDescriptor']['Columns']
        columns_with_data_types = [(column['Name'], column['Type']) for column in table_schema]
        
        # Convert column names to lowercase and cast to appropriate data types
        for col_name, data_type in columns_with_data_types:
            lower_col_name = col_name.lower()
            if lower_col_name in df.columns:
                df = df.withColumn(lower_col_name, spark_col(lower_col_name).cast(data_type))
        
        # Rename columns back to original names
        for original_col_name, new_col_name in zip(column_names_original, df.columns):
            df = df.withColumnRenamed(new_col_name, original_col_name)
    
        # Write DataFrame to Parquet files in specified locations
        df.write.parquet(f"s3://{bucket}/{output_latest}", mode="overwrite")
        df.write.parquet(f"s3://{bucket}/{output_snapshot}", mode="overwrite")
    
        # Log information about the saved results
        logging.info(f"Results have been saved in Parquet format in s3://{bucket}/{output_latest}")
        logging.info(f"Results have been saved in Parquet format in s3://{bucket}/{output_snapshot}")

except Exception as e:
    logging.error(f"An error occurred: {e}")
    traceback_str = ''.join(traceback.format_tb(e.__traceback__))
    logging.error(f"Traceback: {traceback_str}")

finally:
    if conn is not None:
        logging.info("Closing database connection.")
        conn.close()

# Upload logs to S3 at the end of the job
upload_logs_to_s3(bucket, table, log_buffer)

# Mark the job as completed
job.commit()
