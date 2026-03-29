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

# Script generated for node AccelTrusted
AccelTrusted_node1774770336455 = glueContext.create_dynamic_frame.from_catalog(database="trusted", table_name="accelerometer_trusted", transformation_ctx="AccelTrusted_node1774770336455")

# Script generated for node CustTrusted
CustTrusted_node1774770299017 = glueContext.create_dynamic_frame.from_catalog(database="trusted", table_name="customer_trusted", transformation_ctx="CustTrusted_node1774770299017")

# Script generated for node SQL Query
SqlQuery1331 = '''
select distinct CT.* from AT inner join CT on AT.user=CT.email
'''
SQLQuery_node1774770356792 = sparkSqlQuery(glueContext, query = SqlQuery1331, mapping = {"AT":AccelTrusted_node1774770336455, "CT":CustTrusted_node1774770299017}, transformation_ctx = "SQLQuery_node1774770356792")

# Script generated for node CustomerCurated
CustomerCurated_node1774770511043 = glueContext.getSink(path="s3://stedi-landing-zone-udacity/Curated/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1774770511043")
CustomerCurated_node1774770511043.setCatalogInfo(catalogDatabase="curated",catalogTableName="customer_curated")
CustomerCurated_node1774770511043.setFormat("json")
CustomerCurated_node1774770511043.writeFrame(SQLQuery_node1774770356792)
job.commit()