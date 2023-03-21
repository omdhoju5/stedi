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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1679223795855 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-stedi/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1679223795855",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_customer",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Privacy Filter Join
PrivacyFilterJoin_node2 = Join.apply(
    frame1=CustomerTrusted_node1,
    frame2=AccelerometerLanding_node1679223795855,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="PrivacyFilterJoin_node2",
)

# Script generated for node Drop Fields
DropFields_node1679226652165 = DropFields.apply(
    frame=PrivacyFilterJoin_node2,
    paths=[
        "serialnumber",
        "sharewithpublicasofdate",
        "birthday",
        "registrationdate",
        "sharewithresearchasofdate",
        "customername",
        "sharewithfriendsasofdate",
        "email",
        "lastupdatedate",
        "phone",
    ],
    transformation_ctx="DropFields_node1679226652165",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node3 = glueContext.getSink(
    path="s3://udacity-stedi/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node3",
)
AccelerometerTrusted_node3.setCatalogInfo(
    catalogDatabase="stedi_customer", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node3.setFormat("json")
AccelerometerTrusted_node3.writeFrame(DropFields_node1679226652165)
job.commit()
