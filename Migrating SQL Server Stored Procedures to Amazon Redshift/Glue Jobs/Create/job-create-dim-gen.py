import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3


def delete_files(bucket: str, prefix: str) -> None:
    """
    Delete files from S3
    :param bucket: Bucket name
    :param prefix: Prefix
    :return: None
    """
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    bucket.objects.filter(Prefix=prefix).delete()


args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "gluetable", "gluedatabase", "bucket", "outputname"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Read from Data Catalog
DyF = glueContext.create_dynamic_frame.from_catalog(
    database=args["gluedatabase"],
    table_name=args["gluetable"],
    transformation_ctx="DyF",
)

# Script to delete files from S3
delete_files(args["bucket"], args["outputname"])

# Script generated for node Write to S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DyF,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": f"s3://{args['bucket']}/{args['outputname']}/", "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="WriteToS3",
)

job.commit()
