import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from typing import List

def delete_files(bucket: str, prefix: str) -> None:
    """
    Delete files from S3
    :param bucket: Bucket name
    :param prefix: Prefix
    """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    bucket.objects.filter(Prefix=prefix).delete()

# Get job arguments
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "gluetable", "gluedatabase", "bucket"])

# Initialize Glue job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Set outputname
outputname = args["gluetable"].split("_", 1)[-1]

# Read from Data Catalog
dyf = glueContext.create_dynamic_frame.from_catalog(
    database=args["gluedatabase"],
    table_name=args["gluetable"],
    transformation_ctx="DyF",
)

# Delete existing files from S3
delete_files(args["bucket"], outputname)

# Write to S3 bucket
glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": f"s3://{args['bucket']}/{outputname}/",
        "partitionKeys": []
    },
    format_options={"compression": "snappy"},
    transformation_ctx="WriteToS3",
)

job.commit()
