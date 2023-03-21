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
CustomerCurated_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_customer",
    table_name="customer_trusted",
    transformation_ctx="CustomerCurated_node1",
)

# Script generated for node Accelerator Landing
AcceleratorLanding_node1679229501566 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_customer",
    table_name="stedi_accelerometer_landing",
    transformation_ctx="AcceleratorLanding_node1679229501566",
)

# Script generated for node Filter Customer Data
FilterCustomerData_node2 = Join.apply(
    frame1=CustomerCurated_node1,
    frame2=AcceleratorLanding_node1679229501566,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="FilterCustomerData_node2",
)

# Script generated for node Drop Fields
DropFields_node1679229645089 = DropFields.apply(
    frame=FilterCustomerData_node2,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1679229645089",
)

# Script generated for node Customer Curated
CustomerCurated_node3 = glueContext.getSink(
    path="s3://udacity-stedi/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node3",
)
CustomerCurated_node3.setCatalogInfo(
    catalogDatabase="stedi_customer", catalogTableName="customers_curated"
)
CustomerCurated_node3.setFormat("json")
CustomerCurated_node3.writeFrame(DropFields_node1679229645089)
job.commit()
