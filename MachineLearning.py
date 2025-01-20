import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node accel
accel_node1737344348260 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://stedilakehouse/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accel_node1737344348260")

# Script generated for node step
step_node1737344325135 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://stedilakehouse/steptrainer/trusted/"], "recurse": True}, transformation_ctx="step_node1737344325135")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1737344789551 = ApplyMapping.apply(frame=accel_node1737344348260, mappings=[("serialNumber", "string", "right_serialNumber", "string"), ("z", "double", "right_z", "double"), ("birthDay", "string", "right_birthDay", "string"), ("shareWithPublicAsOfDate", "bigint", "right_shareWithPublicAsOfDate", "bigint"), ("shareWithResearchAsOfDate", "bigint", "right_shareWithResearchAsOfDate", "bigint"), ("registrationDate", "bigint", "right_registrationDate", "bigint"), ("customerName", "string", "right_customerName", "string"), ("user", "string", "right_user", "string"), ("shareWithFriendsAsOfDate", "bigint", "right_shareWithFriendsAsOfDate", "bigint"), ("y", "double", "right_y", "double"), ("x", "double", "right_x", "double"), ("timestamp", "bigint", "right_timestamp", "bigint"), ("email", "string", "right_email", "string"), ("lastUpdateDate", "bigint", "right_lastUpdateDate", "bigint"), ("phone", "string", "right_phone", "string")], transformation_ctx="RenamedkeysforJoin_node1737344789551")

# Script generated for node Join
Join_node1737344769487 = Join.apply(frame1=step_node1737344325135, frame2=RenamedkeysforJoin_node1737344789551, keys1=["sensorReadingTime"], keys2=["right_timestamp"], transformation_ctx="Join_node1737344769487")

# Script generated for node MachineLearning
EvaluateDataQuality().process_rows(frame=Join_node1737344769487, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737344314935", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearning_node1737344449525 = glueContext.getSink(path="s3://stedilakehouse/MachineLearning/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearning_node1737344449525")
MachineLearning_node1737344449525.setCatalogInfo(catalogDatabase="stedidatabasev2",catalogTableName="MachineLearning")
MachineLearning_node1737344449525.setFormat("glueparquet", compression="snappy")
MachineLearning_node1737344449525.writeFrame(Join_node1737344769487)
job.commit()