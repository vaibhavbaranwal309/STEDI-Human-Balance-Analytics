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

# Script generated for node CustomerCurated
CustomerCurated_node1774726199730 = glueContext.create_dynamic_frame.from_catalog(database="curated", table_name="customer_curated", transformation_ctx="CustomerCurated_node1774726199730")

# Script generated for node StepTrainerLand
StepTrainerLand_node1774726219317 = glueContext.create_dynamic_frame.from_catalog(database="landing", table_name="step_trainer_landing", transformation_ctx="StepTrainerLand_node1774726219317")

# Script generated for node SQL Query
SqlQuery1330 = '''
select STL.* from CC inner join STL on STL.serialnumber=CC.serialnumber

'''
SQLQuery_node1774726247277 = sparkSqlQuery(glueContext, query = SqlQuery1330, mapping = {"STL":StepTrainerLand_node1774726219317, "CC":CustomerCurated_node1774726199730}, transformation_ctx = "SQLQuery_node1774726247277")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1774726521408 = glueContext.getSink(path="s3://stedi-landing-zone-udacity/Trusted/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1774726521408")
step_trainer_trusted_node1774726521408.setCatalogInfo(catalogDatabase="trusted",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1774726521408.setFormat("json")
step_trainer_trusted_node1774726521408.writeFrame(SQLQuery_node1774726247277)
job.commit()