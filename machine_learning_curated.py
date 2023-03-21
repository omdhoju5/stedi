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

# Script generated for node Curated Customer
CuratedCustomer_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_customer",
    table_name="customers_curated",
    transformation_ctx="CuratedCustomer_node1",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1679385863539 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-stedi/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1679385863539",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1679386099328 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_customer",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1679386099328",
)

# Script generated for node Step Trainer and Accelerometer Join
StepTrainerandAccelerometerJoin_node1679386168974 = Join.apply(
    frame1=StepTrainerLanding_node1679385863539,
    frame2=AccelerometerTrusted_node1679386099328,
    keys1=["sensorReadingTime"],
    keys2=["timestamp"],
    transformation_ctx="StepTrainerandAccelerometerJoin_node1679386168974",
)

# Script generated for node Renamed keys for Join Trusted Step Trainer and Accelerometer
RenamedkeysforJoinTrustedStepTrainerandAccelerometer_node1679386495414 = ApplyMapping.apply(
    frame=StepTrainerandAccelerometerJoin_node1679386168974,
    mappings=[
        ("sensorReadingTime", "long", "`(right) sensorReadingTime`", "long"),
        ("serialNumber", "string", "`(right) serialNumber`", "string"),
        ("distanceFromObject", "int", "`(right) distanceFromObject`", "int"),
        ("serialnumber", "string", "`(right) serialnumber`", "string"),
        ("z", "double", "`(right) z`", "double"),
        ("birthday", "string", "`(right) birthday`", "string"),
        ("timestamp", "long", "`(right) timestamp`", "long"),
        (
            "sharewithresearchasofdate",
            "long",
            "`(right) sharewithresearchasofdate`",
            "long",
        ),
        ("registrationdate", "long", "`(right) registrationdate`", "long"),
        ("customername", "string", "`(right) customername`", "string"),
        ("user", "string", "`(right) user`", "string"),
        ("y", "double", "`(right) y`", "double"),
        ("x", "double", "`(right) x`", "double"),
        ("lastupdatedate", "long", "`(right) lastupdatedate`", "long"),
        (
            "sharewithfriendsasofdate",
            "long",
            "`(right) sharewithfriendsasofdate`",
            "long",
        ),
        (
            "sharewithpublicasofdate",
            "long",
            "`(right) sharewithpublicasofdate`",
            "long",
        ),
    ],
    transformation_ctx="RenamedkeysforJoinTrustedStepTrainerandAccelerometer_node1679386495414",
)

# Script generated for node Join Trusted Step Trainer and Accelerometer
JoinTrustedStepTrainerandAccelerometer_node2 = Join.apply(
    frame1=CuratedCustomer_node1,
    frame2=RenamedkeysforJoinTrustedStepTrainerandAccelerometer_node1679386495414,
    keys1=["email"],
    keys2=["`(right) user`"],
    transformation_ctx="JoinTrustedStepTrainerandAccelerometer_node2",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node3 = glueContext.getSink(
    path="s3://udacity-stedi/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node3",
)
MachineLearningCurated_node3.setCatalogInfo(
    catalogDatabase="stedi_customer", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node3.setFormat("json")
MachineLearningCurated_node3.writeFrame(JoinTrustedStepTrainerandAccelerometer_node2)
job.commit()
