import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer
Accelerometer_node1737333603098 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedilakehouse/accelerometer/landing/"], "recurse": True}, transformation_ctx="Accelerometer_node1737333603098")

# Script generated for node customer
customer_node1737296865940 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedilakehouse/customer/landing/"], "recurse": True}, transformation_ctx="customer_node1737296865940")

# Script generated for node SQL Query
SqlQuery1623 = '''
select * from myDataSource
WHERE shareWithResearchAsOfDate != 0

'''
SQLQuery_node1737332385187 = sparkSqlQuery(glueContext, query = SqlQuery1623, mapping = {"myDataSource":customer_node1737296865940}, transformation_ctx = "SQLQuery_node1737332385187")

# Script generated for node Join
Join_node1737333655908 = Join.apply(frame1=Accelerometer_node1737333603098, frame2=SQLQuery_node1737332385187, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1737333655908")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1737333655908, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737333553830", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1737333776654 = glueContext.getSink(path="s3://stedilakehouse/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1737333776654")
AmazonS3_node1737333776654.setCatalogInfo(catalogDatabase="stedidatabasev2",catalogTableName="AccelerometerTrusted")
AmazonS3_node1737333776654.setFormat("glueparquet", compression="snappy")
AmazonS3_node1737333776654.writeFrame(Join_node1737333655908)
job.commit()