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

# Script generated for node Customer Curated
CustomerCurated_node1679306738777 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_customer",
    table_name="customers_curated",
    transformation_ctx="CustomerCurated_node1679306738777",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity-stedi/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Filter Non Consented Customer
FilterNonConsentedCustomer_node2 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=CustomerCurated_node1679306738777,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="FilterNonConsentedCustomer_node2",
)

# Script generated for node Drop Customer Fields
DropCustomerFields_node1679306951995 = DropFields.apply(
    frame=FilterNonConsentedCustomer_node2,
    paths=[
        "serialnumber",
        "birthday",
        "timestamp",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
        "registrationdate",
        "customername",
        "email",
        "lastupdatedate",
        "phone",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropCustomerFields_node1679306951995",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node3 = glueContext.getSink(
    path="s3://udacity-stedi/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node3",
)
StepTrainerTrusted_node3.setCatalogInfo(
    catalogDatabase="stedi_customer", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node3.setFormat("json")
StepTrainerTrusted_node3.writeFrame(DropCustomerFields_node1679306951995)
job.commit()
