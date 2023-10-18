from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit
import sys


def read_source_data(spark, bucket, mode, read_prefix_raw_statistics, read_prefix_raw_statistics_reference_data):
    raw_statistics_df = spark.read.option("mergeSchema", "true").parquet(f"s3://{bucket}/{mode}/{read_prefix_raw_statistics}")
    raw_statistics_df = raw_statistics_df.withColumnRenamed("title", "title_rs")
    raw_statistics_reference_data_df = spark.read.parquet(f"s3://{bucket}/{mode}/{read_prefix_raw_statistics_reference_data}")
    return raw_statistics_df, raw_statistics_reference_data_df


def write_analysis_df_to_s3(result_df, bucket, mode, write_prefix):
    (result_df.write.partitionBy("process_date")
        .mode("overwrite")
        .format("parquet")
        .save(f"s3://{bucket}/{mode}/{write_prefix}/"))


def job_run(glueContext, bucket, mode, read_prefix_raw_statistics, read_prefix_raw_statistics_reference_data, write_prefix):
    process_date = str(datetime.utcnow().date())
    raw_statistics_df, raw_statistics_reference_data_df = read_source_data(glueContext, bucket, mode, read_prefix_raw_statistics, read_prefix_raw_statistics_reference_data)
    result_df = raw_statistics_df.join(raw_statistics_reference_data_df,
                                       raw_statistics_df.category_id == raw_statistics_reference_data_df.id,
                                       how='inner')
    result_df = result_df.withColumn("process_date", lit(process_date))
    write_analysis_df_to_s3(result_df, bucket, mode, write_prefix)
    job.commit()


if __name__ == "__main__":
    # --JOB_NAME=glue_job_final_daily_analysis should be added to job's environment variable.
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
    bucket = "youtube-project"
    mode = "development"
    read_prefix_raw_statistics = "youtube/raw_statistics_cleaned"
    read_prefix_raw_statistics_reference_data = "youtube/raw_statistics_reference_data_parquet"
    write_prefix = "youtube/final_daily_analysis"
    job_run(glueContext, bucket, mode, read_prefix_raw_statistics, read_prefix_raw_statistics_reference_data, write_prefix)
