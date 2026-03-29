import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node CustLanding
CustLanding_node1774719763874 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-landing-zone-udacity/Landing/customer/"], "recurse": True}, transformation_ctx="CustLanding_node1774719763874")

# Script generated for node Filter
Filter_node1774719818650 = Filter.apply(frame=CustLanding_node1774719763874, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="Filter_node1774719818650")

# Script generated for node CustomerTrusted
CustomerTrusted_node1774719906499 = glueContext.getSink(path="s3://stedi-landing-zone-udacity/Trusted/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1774719906499")
CustomerTrusted_node1774719906499.setCatalogInfo(catalogDatabase="trusted",catalogTableName="customer_trusted")
CustomerTrusted_node1774719906499.setFormat("json")
CustomerTrusted_node1774719906499.writeFrame(Filter_node1774719818650)
job.commit()