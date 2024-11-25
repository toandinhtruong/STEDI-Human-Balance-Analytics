import sys
from awsglue.transforms import Filter, Join
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions

# Retrieve job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

# Load customer landing data from S3
customer_data = glue_context.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://toantd19-bucket/customer/landing/"],
        "recurse": True
    },
    transformation_ctx="customer_data"
)

# Filter out customers who have not consented for research sharing
filtered_customer_data = Filter.apply(
    frame=customer_data,
    f=lambda row: row["shareWithResearchAsOfDate"] != 0,
    transformation_ctx="filtered_customer_data"
)

# Load accelerometer data from S3
accelerometer_data = glue_context.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://toantd19-bucket/accelerometer/landing/"],
        "recurse": True
    },
    transformation_ctx="accelerometer_data"
)

# Perform the join between filtered customer data and accelerometer data
joined_data = Join.apply(
    frame1=filtered_customer_data,
    frame2=accelerometer_data,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="joined_data"
)

# Write the joined and cleaned data to the curated S3 path
curated_output = glue_context.write_dynamic_frame.from_options(
    frame=joined_data,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://toantd19-bucket/curated/",
        "partitionKeys": []
    },
    transformation_ctx="curated_output"
)

# Write the joined data to Glue Catalog as a curated table
glue_context.write_dynamic_frame.from_catalog(
    frame=joined_data,
    database="toantd19”,
    table_name="customer_curated",
    transformation_ctx="customer_curated_output"
)

# Commit the job to finalize execution
job.commit()
