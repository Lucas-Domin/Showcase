import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import boto3
import datetime
from typing import Dict

def sparkSqlQuery(glueContext: GlueContext, query: str, mapping: Dict[str, DynamicFrame], transformation_ctx: str) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

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

# Read from Data Catalog
dyf = glueContext.create_dynamic_frame.from_catalog(
    database=args["gluedatabase"],
    table_name=args["gluetable"],
    transformation_ctx="DyF",
)

# Extract list with unique values
df_unique_values_dates = dyf.toDF().select('month_column').distinct()
list_unique_values_dates = df_unique_values_dates.rdd.map(lambda x: x.month_column).collect()

for u_date in list_unique_values_dates:
    date = datetime.datetime.strptime(str(u_date), "%Y%m")
    outputname = f'{args["gluetable"].split("_", 1)[-1]}/year={date.year}/month={date.month:02d}'
    
    # Filter DynamicFrame
    filtered_dyf = Filter.apply(
        frame=dyf,
        f=lambda row: (row["month_column"] == u_date),
        transformation_ctx="Filtered_DyF",
    )
    
    # Delete existing files from S3
    delete_files(args["bucket"], outputname)
    
    # Write to S3 bucket
    glueContext.write_dynamic_frame.from_options(
        frame=filtered_dyf,
        connection_type="s3",
        format="parquet",
        connection_options={
            "path": f"s3://{args['bucket']}/{outputname}/",
            "partitionKeys": []
        },
        format_options={"compression": "snappy"},
        transformation_ctx="S3bucket_write",
    )

job.commit()
