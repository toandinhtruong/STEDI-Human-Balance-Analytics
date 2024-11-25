import sys
from awsglue.transforms import Filter
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions

# Retrieve the job name and initialize context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

# Step 1: Load customer data from the landing zone (S3)
customer_data_landing = glue_context.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://toantd19-bucket/customer/landing/"],
        "recurse": True
    },
    transformation_ctx="customer_data_landing"
)

# Step 2: Filter out customers who have opted out of research sharing (shareWithResearchAsOfDate == 0)
filtered_customer_data = Filter.apply(
    frame=customer_data_landing,
    f=lambda row: row["shareWithResearchAsOfDate"] != 0,
    transformation_ctx="filtered_customer_data"
)

# Step 3: Write the filtered customer data to the trusted zone in S3 (curated data)
trusted_customer_data_output = glue_context.write_dynamic_frame.from_options(
    frame=filtered_customer_data,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://toantd19-bucket/customer/trusted/",
        "partitionKeys": []
    },
    transformation_ctx="trusted_customer_data_output"
)

# Step 4: Commit the job
job.commit()

