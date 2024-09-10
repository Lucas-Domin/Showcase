import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.types import IntegerType

def transform_datatypes(dynamic_frame):
    df = dynamic_frame.toDF()
    for column_name, data_type in df.dtypes:
        if data_type == 'smallint':
            df = df.withColumn(column_name, df[column_name].cast(IntegerType()))
    return DynamicFrame.fromDF(df, glueContext, 'transformed_frame')

def generate_create_table_sql(dyf, target_table_name) -> str:
    """
    Generate SQL statement to create table
    :param dyf: DynamicFrame
    :param target_table_name: Target table name
    :return: SQL statement to create table
    """
    # Define the mapping between source and target data types
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
        'long': 'BIGINT'
    }
    
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

def extract_distinct_values(dynamic_frame, column_name):
    # Convert the DynamicFrame to a DataFrame
    dataframe = dynamic_frame.toDF()
    # Get the distinct values of the specified column
    distinct_values = dataframe.select(column_name).distinct().collect()
    # Extract the distinct values from the result
    distinct_values = [int(row[column_name]) for row in distinct_values]
    return distinct_values

# Parse job arguments
args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "gluedatabase", "gluetable", "redshiftconnection", "redshifttable"
])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read data from source
source_frame = glueContext.create_dynamic_frame.from_catalog(
    database=args["gluedatabase"],
    table_name=args["gluetable"],
    transformation_ctx="ReadSource",
)

distinct_values = extract_distinct_values(source_frame, "ProcessMonth")

# Transform data types
transformed_frame = transform_datatypes(source_frame)

# Write to target
target_frame = glueContext.write_dynamic_frame.from_options(
    frame=transformed_frame,
    connection_type="redshift",
    connection_options={
        "redshiftTmpDir": "s3://temporarypath/",
        "useConnectionProperties": "true",
        "dbtable": args["redshiftconnection"],
        "connectionName": args["redshifttable"],
        "preactions": f"{generate_create_table_sql(transformed_frame, args['redshifttable'])}; DELETE FROM {args['redshifttable']} WHERE ProcessMonth IN ({', '.join([str(x) for x in distinct_values])});",
    },
    transformation_ctx="WriteTarget",
)

job.commit()
