import sys
from awsglue.context import GlueContext
from awsglue.transforms import DropFields, Filter, Join
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

# Load customer landing data
customer_data = glue_context.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://toantd19-bucket/customer/landing/"]},
    transformation_ctx="customer_data"
)

# Filter out customers based on 'shareWithResearchAsOfDate'
filtered_customer_data = customer_data.filter(
    lambda row: row["shareWithResearchAsOfDate"] != 0
)

# Load accelerometer data
accelerometer_data = glue_context.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://toantd19-bucket/accelerometer/landing/"]},
    transformation_ctx="accelerometer_data"
)

# Join customer and accelerometer data
joined_data = filtered_customer_data.join(
    frame2=accelerometer_data,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="joined_data"
)

# Drop unnecessary columns after the join
cleaned_data = DropFields.apply(
    frame=joined_data,
    paths=[
        "customerName", "email", "phone", "birthDay", "serialNumber", 
        "shareWithFriendsAsOfDate", "shareWithPublicAsOfDate", 
        "shareWithResearchAsOfDate", "registrationDate", "lastUpdateDate"
    ],
    transformation_ctx="cleaned_data"
)

# Write the processed data to S3
glue_context.write_dynamic_frame.from_options(
    frame=cleaned_data,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://toantd19-bucket/accelerometer/trusted/"},
    transformation_ctx="write_to_s3"
)

# Optionally, write to Glue catalog
glue_context.write_dynamic_frame.from_catalog(
    frame=cleaned_data,
    database="toantd19",
    table_name="accelerometer_trusted",
    transformation_ctx="write_to_catalog"
)

# Commit the job
job.commit()

