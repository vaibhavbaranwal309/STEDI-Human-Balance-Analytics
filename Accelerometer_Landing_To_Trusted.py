import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node CustTrusted
CustTrusted_node1774724761109 = glueContext.create_dynamic_frame.from_catalog(database="trusted", table_name="customer_trusted", transformation_ctx="CustTrusted_node1774724761109")

# Script generated for node AccelerometerLand
AccelerometerLand_node1774724728887 = glueContext.create_dynamic_frame.from_catalog(database="landing", table_name="accelerometer_landing", transformation_ctx="AccelerometerLand_node1774724728887")

# Script generated for node SQL Query
SqlQuery1311 = '''
select Al.user,AL.timestamp ,AL.x,AL.y,AL.z from AL inner join CT on AL.user=CT.email
where Al.timestamp>CT.sharewithresearchasofdate
'''
SQLQuery_node1774725452612 = sparkSqlQuery(glueContext, query = SqlQuery1311, mapping = {"AL":AccelerometerLand_node1774724728887, "CT":CustTrusted_node1774724761109}, transformation_ctx = "SQLQuery_node1774725452612")

# Script generated for node Accelerometer_Trusted
Accelerometer_Trusted_node1774724896165 = glueContext.getSink(path="s3://stedi-landing-zone-udacity/Trusted/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Accelerometer_Trusted_node1774724896165")
Accelerometer_Trusted_node1774724896165.setCatalogInfo(catalogDatabase="trusted",catalogTableName="accelerometer_trusted")
Accelerometer_Trusted_node1774724896165.setFormat("json")
Accelerometer_Trusted_node1774724896165.writeFrame(SQLQuery_node1774725452612)
job.commit()