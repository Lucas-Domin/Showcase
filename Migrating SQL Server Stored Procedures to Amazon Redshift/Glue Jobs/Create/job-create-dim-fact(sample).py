from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from pyspark.sql.functions import *
from awsglue.utils import getResolvedOptions
import sys

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket', 'outputname'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Helper function to delete files
def delete_files(bucket, prefix):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket)
    bucket.objects.filter(Prefix=prefix).delete()

# Node: Rename Product Info
Rename_Product_Info = RenameField.apply(
    frame=Product_Info,
    old_name="Product_Year",
    new_name="Product_Annum",
    transformation_ctx="Rename_Product_Info",
)

# Node: Trim Customer_Data.Customer_ID
Trim_Customer_Data = trimColumns(glueContext=glueContext,
                                 cols=["Customer_ID"],
                                 frame=Customer_Data,
                                 transformation_ctx="Trim_Customer_Data")

# Node: Regular_Customers
SqlQuery1 = """
SELECT 
  1 AS Customer_Type, 
  C.Company_ID, 
  C.Company_Version, 
  C.Branch_ID, 
  C.Branch_Version, 
  C.Contract_Number AS ContractNum, 
  C.Contract_Year AS ContractYear, 
  0 AS OfferNum, 
  0 AS Territory_ID, 
  C.Start_Date AS StartDate, 
  C.End_Date AS EndDate, 
  IFNULL(P.Partner_ID, 0) AS Partner_ID
FROM 
  Customer_Data C
  LEFT JOIN Partner_Info P ON C.Company_ID = P.Company_ID 
  AND C.Company_Version = P.Company_Version 
  AND C.Branch_ID = P.Branch_ID 
  AND C.Branch_Version = P.Branch_Version 
  AND C.Contract_Number = P.Contract_Number 
  AND C.Contract_Year = P.Contract_Year
"""
Regular_Customers = sparkSqlQuery(
    glueContext,
    query=SqlQuery1,
    mapping={
        "Customer_Data": Customer_Data,
        "Partner_Info": Partner_Info,
    },
    transformation_ctx="Regular_Customers",
)

# Node: Special_Customers
SqlQuery2 = """
SELECT 
  2 AS Customer_Type, 
  S.Company_ID, 
  S.Company_Version, 
  S.Branch_ID, 
  S.Branch_Version, 
  S.Contract_Number, 
  S.Contract_Year, 
  0 AS tmp1, 
  0 AS tmp2, 
  S.Start_Date, 
  S.End_Date, 
  IFNULL(P.Partner_ID, 0) AS Partner_ID 
FROM 
  Special_Customer_Data S
  LEFT JOIN Partner_Info P ON S.Company_ID = P.Company_ID 
  AND S.Company_Version = P.Company_Version 
  AND S.Branch_ID = P.Branch_ID 
  AND S.Branch_Version = P.Branch_Version 
  AND S.Contract_Number = P.Contract_Number 
  AND S.Contract_Year = P.Contract_Year
"""
Special_Customers = sparkSqlQuery(
    glueContext,
    query=SqlQuery2,
    mapping={
        "Special_Customer_Data": Special_Customer_Data,
        "Partner_Info": Partner_Info,
    },
    transformation_ctx="Special_Customers",
)

# Node: All_Customers
SqlQuery3 = """
SELECT * FROM Regular_Customers
UNION ALL
SELECT * FROM Special_Customers
"""
All_Customers = sparkSqlQuery(
    glueContext,
    query=SqlQuery3,
    mapping={
        "Regular_Customers": Regular_Customers,
        "Special_Customers": Special_Customers,
    },
    transformation_ctx="All_Customers",
)

# Node: Transaction_Data_Extract
SqlQuery4 = f"""
SELECT 
  A.Account_Code, 
  T.Transaction_Type, 
  T.Payment_Method, 
  T.Is_Capital, 
  T.Transaction_ID, 
  T.Policy_Number, 
  T.Process_Month, 
  T.Transaction_Nature, 
  T.Customer_Type, 
  T.Process_Number, 
  CASE WHEN T.Transaction_Nature IN ('IN', 'EX', 'OT') THEN T.Company_ID ELSE T.Original_Company_ID END AS Company_ID, 
  CASE WHEN T.Transaction_Nature IN ('IN', 'EX', 'OT') THEN T.Company_Version ELSE T.Original_Company_Version END AS Company_Version, 
  CASE WHEN T.Transaction_Nature IN ('IN', 'EX', 'OT') THEN T.Branch_ID ELSE T.Original_Branch_ID END AS Branch_ID, 
  CASE WHEN T.Transaction_Nature IN ('IN', 'EX', 'OT') THEN T.Branch_Version ELSE T.Original_Branch_Version END AS Branch_Version, 
  CASE WHEN T.Transaction_Nature IN ('IN', 'EX', 'OT') THEN T.Contract_Number ELSE T.Original_Contract_Number END AS Contract_Number, 
  CASE WHEN T.Transaction_Nature IN ('IN', 'EX', 'OT') THEN T.Contract_Year ELSE T.Original_Contract_Year END AS Contract_Year, 
  T.Country_ID,
  T.Customer_ID as Transaction_Customer_ID, 
  T.Product_ID, 
  T.Product_Number, 
  T.Product_Year, 
  T.Category_ID, 
  T.Subcategory_ID, 
  T.Currency_ID, 
  T.Claim_Number, 
  T.Partner_ID, 
  SUM(T.Amount_Tech) AS Amount_Tech, 
  SUM(CASE
        WHEN A.Account_Code IN (2125, 3410, 1706) THEN E2.Exchange_Rate * T.Amount_Balance 
        WHEN A.Account_Code IN (5201, 5207, 5209) THEN T.Amount_Balance 
        ELSE E1.Exchange_Rate * T.Amount_Balance
        END) AS Amount_National
FROM 
  Transaction_Data T 
  INNER JOIN Account_Info A ON A.Account_Code IN (
    1602, 1604, 1701, 1703, 1706, 1710, 2121, 
    2125, 3410, 6102, 6104, 6108, 6111, 
    5301, 5310, 5311, 5312, 5102, 5104, 
    5108, 5111, 6302, 6304, 5402, 5404, 
    6402, 6404, 6414, 6415, 6416, 6417, 
    5201, 5207, 5209
  )
  AND A.Account_ID = T.Account_ID 
  AND A.Account_Version = T.Account_Version 
  
  INNER JOIN Exchange_Rates E1 
  ON E1.Date = T.Process_Month 
  AND E1.Currency_ID = T.Balance_Currency_ID 

  INNER JOIN Exchange_Rates E2 
  ON E2.Date = year(add_months(to_date(string(T.Process_Month*100+01), 'yyyyMMdd'),-1))*100 + month(add_months(to_date(string(T.Process_Month*100+01), 'yyyyMMdd'),-1))
  AND E2.Currency_ID = T.Balance_Currency_ID 
  
  WHERE ((T.Process_Month <= 202201) OR (A.Account_Code IN (5201, 5207, 5209, 2125, 3410)) OR (A.Account_Code IN (1706) AND T.Transaction_Type IN (6)))
  
GROUP BY 
  A.Account_Code, 
  T.Transaction_Type, 
  T.Payment_Method, 
  T.Is_Capital, 
  T.Transaction_ID, 
  T.Policy_Number, 
  T.Process_Month, 
  T.Transaction_Nature, 
  T.Customer_Type, 
  T.Process_Number, 
  CASE WHEN T.Transaction_Nature IN ('IN', 'EX', 'OT') THEN T.Company_ID ELSE T.Original_Company_ID END, 
  CASE WHEN T.Transaction_Nature IN ('IN', 'EX', 'OT') THEN T.Company_Version ELSE T.Original_Company_Version END, 
  CASE WHEN T.Transaction_Nature IN ('IN', 'EX', 'OT') THEN T.Branch_ID ELSE T.Original_Branch_ID END, 
  CASE WHEN T.Transaction_Nature IN ('IN', 'EX', 'OT') THEN T.Branch_Version ELSE T.Original_Branch_Version END, 
  CASE WHEN T.Transaction_Nature IN ('IN', 'EX', 'OT') THEN T.Contract_Number ELSE T.Original_Contract_Number END, 
  CASE WHEN T.Transaction_Nature IN ('IN', 'EX', 'OT') THEN T.Contract_Year ELSE T.Original_Contract_Year END, 
  T.Country_ID, 
  T.Customer_ID,
  T.Product_ID, 
  T.Product_Number, 
  T.Product_Year, 
  T.Category_ID, 
  T.Subcategory_ID, 
  T.Currency_ID, 
  T.Claim_Number, 
  T.Partner_ID
"""
Transaction_Data_Extract = sparkSqlQuery(
    glueContext,
    query=SqlQuery4,
    mapping={
        "Transaction_Data": Trim_Customer_Data,
        "Account_Info": Account_Info,
        "Exchange_Rates": Exchange_Rates
    },
    transformation_ctx="Transaction_Data_Extract",
)

# Node: Fact_Table_Final
SqlQuery5 = """
SELECT 
    T.Account_Code,
    T.Transaction_Type,
    T.Payment_Method,
    T.Is_Capital,
    T.Transaction_ID,
    T.Policy_Number,
    T.Process_Month,
    T.Transaction_Nature,
    T.Customer_Type,
    T.Process_Number,
    T.Company_ID,
    T.Company_Version,
    T.Branch_ID,
    T.Branch_Version,
    T.Contract_Number,
    T.Contract_Year,
    T.Country_ID,
    T.Transaction_Customer_ID,
    T.Product_ID,
    T.Product_Number,
    T.Product_Year,
    T.Category_ID,
    T.Subcategory_ID,
    T.Currency_ID,
    T.Claim_Number,
    T.Partner_ID,
    T.Amount_Tech,
    T.Amount_National,
    C.Customer_Type AS Contract_Type,
    C.Start_Date,
    C.End_Date
FROM 
    Transaction_Data_Extract T
LEFT JOIN 
    All_Customers C
ON 
    T.Company_ID = C.Company_ID
    AND T.Company_Version = C.Company_Version
    AND T.Branch_ID = C.Branch_ID
    AND T.Branch_Version = C.Branch_Version
    AND T.Contract_Number = C.ContractNum
    AND T.Contract_Year = C.ContractYear
"""
Fact_Table_Final = sparkSqlQuery(
    glueContext,
    query=SqlQuery5,
    mapping={"Transaction_Data_Extract": Transaction_Data_Extract, "All_Customers": All_Customers},
    transformation_ctx="Fact_Table_Final",
)

# Node: Delete folder in S3
delete_files(args["bucket"], args["outputname"])

# Node: Write data to S3
AmazonS3 = glueContext.write_dynamic_frame.from_options(
    frame=Fact_Table_Final,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": f"s3://{args['bucket']}/{args['outputname']}/",
        "partitionKeys": [],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="Write data to S3",
)

job.commit()
