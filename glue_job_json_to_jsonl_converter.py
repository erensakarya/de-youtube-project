import sys
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, explode


def read_json_data(spark, bucket, mode, read_prefix):
    path = f"s3://{bucket}/{mode}/{read_prefix}/"
    json_df = spark.read.option("multiline","true").json(path)
    json_df.show(1, truncate=False)
    return json_df
    
    
def drop_unnecessary_columns(json_df):
    json_df_unnecessary_columns = ["etag", "kind"]
    json_df = json_df.drop(*json_df_unnecessary_columns)
    json_df.show(1, truncate=False)
    return json_df
   
    
def flatten_array_column(json_df):
    json_df = (json_df.select(explode('items').alias('items'))
      .select('items.etag', 'items.id', 'items.kind', 'items.snippet.assignable', 'items.snippet.channelid', 'items.snippet.title'))
    json_df.show(1, truncate=False)
    return json_df
    
    
def write_df_as_parquet_to_s3(json_df, bucket, mode, write_prefix):
    json_df.coalesce(1).write.mode("overwrite").format("parquet").save(f"s3://{bucket}/{mode}/{write_prefix}/")


def job_run(spark, bucket, mode, read_prefix, write_prefix):
    json_df = read_json_data(spark, bucket, mode, read_prefix)
    json_df = drop_unnecessary_columns(json_df)
    json_df = flatten_array_column(json_df)
    write_df_as_parquet_to_s3(json_df, bucket, mode, write_prefix)


if __name__ == "__main__":
    # --JOB_NAME=glue_job_json_to_jsonl_converter should be added to job's environment variable.
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
    bucket = "youtube-project"
    mode = "development"
    read_prefix = "youtube/raw_statistics_reference_data_json"
    write_prefix = "youtube/raw_statistics_reference_data_parquet"
    job_run(spark, bucket, mode, read_prefix, write_prefix)
