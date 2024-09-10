import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.types import BinaryType, StringType, IntegerType

def transform_data_types(dynamic_frame):
    df = dynamic_frame.toDF()
    for column_name, data_type in df.dtypes:
        if data_type == 'smallint':
            df = df.withColumn(column_name, df[column_name].cast(IntegerType()))
        elif data_type == 'binary':
            df = df.withColumn(column_name, df[column_name].cast(StringType()))
    return DynamicFrame.fromDF(df, glueContext, 'transformed_frame')
    
def generate_create_table_sql(dyf, target_table_name) -> str:
    """
    Generate SQL statement to create table
    :param dyf: DynamicFrame
    :param target_table_name: Target table name
    :return: SQL statement to create table
    """
    # Define the mapping between the source and target data types
    type_mapping = {
        'binary': 'BYTEA',
        'boolean': 'BOOLEAN',
        'tinyint': 'SMALLINT',
        'smallint': 'SMALLINT',
        'int': 'INTEGER',
        'bigint': 'BIGINT',
        'float': 'REAL',
        'double': 'DOUBLE PRECISION',
        'decimal': 'DECIMAL',
        'timestamp': 'TIMESTAMP',
        'date': 'DATE',
        'string': 'VARCHAR',
        'short': 'SMALLINT',
        'long': 'BIGINT'}
    
    # Extract schema information
    schema = dyf.schema()
    
    # Generate column definitions
    column_defs = []
    for field in schema.fields:
        col_name = field.name
        source_type = field.dataType.typeName()
        target_type = type_mapping.get(source_type, 'VARCHAR')
        column_defs.append(f"{col_name} {target_type}")
    
    # Construct the SQL statement
    create_table_sql = "CREATE TABLE IF NOT EXISTS {} ({})".format(
        target_table_name, ", ".join(column_defs))
    
    return create_table_sql

# Parse job arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_db", "source_table", "target_conn", "target_table"])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read data from source
source_frame = glueContext.create_dynamic_frame.from_catalog(
    database=args["source_db"],
    table_name=args["source_table"],
    transformation_ctx="ReadSource",
)

# Transform data types
transformed_frame = transform_data_types(source_frame)

# Write to target
target_frame = glueContext.write_dynamic_frame.from_options(
    frame=transformed_frame,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://temp-storage/",
        "useConnectionProperties": "true",
        "dbtable": args["target_table"],
        "connectionName": args["target_conn"],
        "preactions": f"{generate_create_table_sql(transformed_frame, args['target_table'])}; TRUNCATE TABLE {args['target_table']};",
    },
    transformation_ctx="WriteTarget",
)

job.commit()
