import sys
from awsglue.transforms import Filter, Join, ApplyMapping, DropFields
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions

# Initialize the job arguments and Spark context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args['JOB_NAME'], args)

# Step 1: Load customer data from S3 landing zone
customer_landing_data = glue_context.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://toantd19-bucket/customer/landing/"],
        "recurse": True
    },
    transformation_ctx="customer_landing_data"
)

# Step 2: Filter customers who consented for research sharing
filtered_customer_data = Filter.apply(
    frame=customer_landing_data,
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

# Step 4: Load step trainer data
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
customer_accelerometer_data = Join.apply(
    frame1=filtered_customer_data,
    frame2=accelerometer_data,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="customer_accelerometer_data"
)

# Step 6: Rename keys for step trainer data to align with customer data
renamed_step_trainer_data = ApplyMapping.apply(
    frame=step_trainer_data,
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

# Step 7: Join step trainer data with the previous data based on serial number
step_trainer_joined_data = Join.apply(
    frame1=step_trainer_data,
    frame2=renamed_step_trainer_data,
    keys1=["serialNumber"],
    keys2=["right_serialNumber"],
    transformation_ctx="step_trainer_joined_data"
)

# Step 8: Join the step trainer data with the machine learning data based on timestamp
final_ml_data = Join.apply(
    frame1=step_trainer_joined_data,
    frame2=renamed_step_trainer_data,
    keys1=["sensorReadingTime"],
    keys2=["right_timeStamp"],
    transformation_ctx="final_ml_data"
)

# Step 9: Drop unnecessary fields to prepare the data for machine learning
curated_ml_data = DropFields.apply(
    frame=final_ml_data,
    paths=[
        "right_customerName", "right_email", "right_phone", "right_birthDay", 
        "right_serialNumber", "right_registrationDate", "right_lastUpdateDate", 
        "right_shareWithResearchAsOfDate", "right_shareWithPublicAsOfDate", 
        "right_shareWithFriendsAsOfDate", "right_user", "right_timeStamp", 
        "right_x", "right_y", "right_z"
    ],
    transformation_ctx="curated_ml_data"
)

# Step 10: Write the final curated data to S3 for machine learning consumption
glue_context.write_dynamic_frame.from_options(
    frame=curated_ml_data,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://toantd19-bucket/machine_learning/",
        "partitionKeys": []
    },
    transformation_ctx="ml_output"
)

# Step 11: Commit the job to complete execution
job.commit()

