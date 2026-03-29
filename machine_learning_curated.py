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
AccelTrusted_node1774771756212 = glueContext.create_dynamic_frame.from_catalog(database="trusted", table_name="accelerometer_trusted", transformation_ctx="AccelTrusted_node1774771756212")

# Script generated for node StepTrainerTrusted
StepTrainerTrusted_node1774771731396 = glueContext.create_dynamic_frame.from_catalog(database="trusted", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1774771731396")

# Script generated for node SQL Query
SqlQuery1483 = '''
select * from AT inner join ST on ST.sensorreadingtime=AT.timestamp

'''
SQLQuery_node1774771780994 = sparkSqlQuery(glueContext, query = SqlQuery1483, mapping = {"AT":AccelTrusted_node1774771756212, "ST":StepTrainerTrusted_node1774771731396}, transformation_ctx = "SQLQuery_node1774771780994")

# Script generated for node machine_learning_curated
machine_learning_curated_node1774772022526 = glueContext.getSink(path="s3://stedi-landing-zone-udacity/Curated/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1774772022526")
machine_learning_curated_node1774772022526.setCatalogInfo(catalogDatabase="curated",catalogTableName="machine_learning_curated")
machine_learning_curated_node1774772022526.setFormat("json")
machine_learning_curated_node1774772022526.writeFrame(SQLQuery_node1774771780994)
job.commit()