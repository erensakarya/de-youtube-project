from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import col, input_file_name, split
import sys


def read_csv_data(glueContext, bucket, mode, read_prefix):
    csv_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            'paths': [f"s3://{bucket}/{mode}/{read_prefix}/"],
            'recurse': True},
        format="csv",
        format_options={"withHeader": True},
        transformation_ctx="dyf")
    csv_df = csv_dyf.toDF()
    csv_df = csv_df.withColumn("file_name", input_file_name())
    csv_df = csv_df.withColumn("region", split(split(col("file_name"), '/').getItem(6), "=").getItem(1))
    csv_df = csv_df.drop("file_name")
    return csv_df


def write_df_as_parquet_to_s3(csv_df, bucket, mode, write_prefix):
    (csv_df.repartition("region")
        .write.partitionBy("region")
        .mode("overwrite")
        .format("parquet")
        .save(f"s3://{bucket}/{mode}/{write_prefix}/"))


def job_run(glueContext, bucket, mode, read_prefix, write_prefix):
    csv_df = read_csv_data(glueContext, bucket, mode, read_prefix)
    write_df_as_parquet_to_s3(csv_df, bucket, mode, write_prefix)
    job.commit()


if __name__ == "__main__":
    # --JOB_NAME=glue_job_raw_statistics_cleaner should be added to job's environment variable.
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
    bucket = "youtube-project"
    mode = "development"
    read_prefix = "youtube/raw_statistics"
    write_prefix = "youtube/raw_statistics_cleaned"
    job_run(glueContext, bucket, mode, read_prefix, write_prefix)
