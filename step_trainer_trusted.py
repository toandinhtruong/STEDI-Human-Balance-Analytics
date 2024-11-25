import sys
from awsglue.transforms import Filter, Join, ApplyMapping
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions

# Retrieve job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

# Step 1: Load customer data from the landing zone
customer_data = glue_context.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://toantd19-bucket/customer/landing/"],
        "recurse": True
    },
    transformation_ctx="customer_data"
)

# Step 2: Filter out customers who have consented for research sharing
filtered_customer_data = Filter.apply(
    frame=customer_data,
    f=lambda row: row["shareWithResearchAsOfDate"] != 0,
    transformation_ctx="filtered_customer_data"
)

# Step 3: Load accelerometer data from the landing zone
accelerometer_data = glue_context.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://toantd19-bucket/accelerometer/landing/"],
        "recurse": True
    },
    transformation_ctx="accelerometer_data"
)

# Step 4: Load step trainer data from the landing zone
step_trainer_data = glue_context.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://toantd19-bucket/step_trainer/landing/"],
        "recurse": True
    },
    transformation_ctx="step_trainer_data"
)

# Step 5: Join filtered customer data with accelerometer data
joined_accel_customer_data = Join.apply(
    frame1=filtered_customer_data,
    frame2=accelerometer_data,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="joined_accel_customer_data"
)

# Step 6: Rename fields in the step trainer data for consistency
renamed_step_trainer_data = ApplyMapping.apply(
    frame=joined_accel_customer_data,
    mappings=[
        ("customerName", "string", "right_customerName", "string"),
        ("email", "string", "right_email", "string"),
        ("phone", "string", "right_phone", "string"),
        ("birthDay", "string", "right_birthDay", "string"),
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("registrationDate", "bigint", "right_registrationDate", "bigint"),
        ("lastUpdateDate", "bigint", "right_lastUpdateDate", "bigint"),
        ("shareWithResearchAsOfDate", "bigint", "right_shareWithResearchAsOfDate", "bigint"),
        ("shareWithPublicAsOfDate", "bigint", "right_shareWithPublicAsOfDate", "bigint"),
        ("shareWithFriendsAsOfDate", "bigint", "right_shareWithFriendsAsOfDate", "bigint"),
        ("user", "string", "right_user", "string"),
        ("timeStamp", "bigint", "right_timeStamp", "bigint"),
        ("x", "double", "right_x", "double"),
        ("y", "double", "right_y", "double"),
        ("z", "double", "right_z", "double")
    ],
    transformation_ctx="renamed_step_trainer_data"
)

# Step 7: Join the renamed step trainer data with the accelerometer data
joined_step_trainer_data = Join.apply(
    frame1=step_trainer_data,
    frame2=renamed_step_trainer_data,
    keys1=["serialNumber"],
    keys2=["right_serialNumber"],
    transformation_ctx="joined_step_trainer_data"
)

# Step 8: Write filtered accelerometer data to the curated S3 location
glue_context.write_dynamic_frame.from_options(
    frame=joined_accel_customer_data,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://toantd19-bucket/curated/",
        "partitionKeys": []
    },
    transformation_ctx="curated_accel_data"
)

# Step 9: Write the joined step trainer data to the 'step_trainer_trusted' table in Glue catalog
glue_context.write_dynamic_frame.from_catalog(
    frame=joined_step_trainer_data,
    database="tuanpa40",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_data"
)

# Step 10: Write the step trainer data to the curated S3 location
glue_context.write_dynamic_frame.from_options(
    frame=joined_step_trainer_data,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://toantd19-bucket/curated_trusted/",
        "partitionKeys": []
    },
    transformation_ctx="curated_trusted_step_trainer_data"
)

# Commit the job to complete the process
job.commit()

