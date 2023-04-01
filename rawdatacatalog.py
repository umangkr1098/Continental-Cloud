import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ";",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://demodatabarcalys/rawdata/"], "recurse": True},
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("age", "string", "age", "long"),
        ("job", "string", "job", "string"),
        ("marital", "string", "marital", "string"),
        ("education", "string", "education", "string"),
        ("default", "string", "default", "string"),
        ("balance", "string", "balance", "long"),
        ("housing", "string", "housing", "string"),
        ("loan", "string", "loan", "string"),
        ("contact", "string", "contact", "string"),
        ("day", "string", "day", "long"),
        ("month", "string", "month", "string"),
        ("duration", "string", "duration", "long"),
        ("campaign", "string", "campaign", "long"),
        ("pdays", "string", "pdays", "long"),
        ("previous", "string", "previous", "long"),
        ("poutcome", "string", "poutcome", "string"),
        ("y", "string", "y", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Data Catalog
DataCatalog_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="rawdb",
    table_name="rawdata",
    additional_options={"enableUpdateCatalog": True, "updateBehavior": "LOG"},
    transformation_ctx="DataCatalog_node3",
)

job.commit()
