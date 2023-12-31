# de-youtube-project
Data Engineering Project with Youtube data by using AWS.

### Technology Stack:
* Python
* Pyspark
* Bash Script
* AWS Glue
* AWS Lambda
* AWS S3
* AWS Athena
* yaml

### <ins>Project Overview
We want to simulate a scenario like below;

* *Our management team wants to launch a new data-driven campaign and our main advertising channel is 'Youtube'.<br>
So I decided to use a Kaggle Youtube dataset for this certain scenario.*

https://www.kaggle.com/datasets/datasnaek/youtube-new

We will answer 2 questions in this project with the Kaggle data:
* How to categorise videos, based on their comments and statistics.
* What factors affect how popular a Youtube video will be.

Kaggle Youtube dataset is a daily record of the top trending YouTube videos and includes several months of data on daily trending YouTube videos. Data is included for some countries like the US, GB, DE etc... <br>
Our dataset has 2 file types: .csv and .json.


## <ins>Project
#### 1- Create a S3 bucket named "youtube-project" and create a Glue Database named "youtube_project" and Glue Tables with Glue Crawlers to be able to run queries with AWS Athena.
Download the Youtube data and upload to "youtube-project" S3 bucket with commands which are in [upload_commands.sh](https://github.com/erensakarya/de-youtube-project/blob/main/upload_commands.sh) file.<br>
Create glue crawlers for each data so tables can be created in Glue Catalog like below;
* youtube_project_raw_statistics_crawler for raw_statistics data and raw_statistics table with region partition.
* youtube_project_raw_statistics_reference_data_crawler for raw_statistics_reference_data data and raw_statistics_reference_data table with no partition.

Both tables will be created in the Glue Catalog but while 1st table can be queried successfully with AWS Athena (since background files are .csv), 2nd table can't be queried with Athena. Because the background .json datas are not .jsonl (new line delimited json files), instead they are old fashioned multiline .json files. Therefore, Athena can't handle these type .json files.<br>
To handle this sitution, we have 3 options like below;
* a- Convert .json files to .jsonl files. json to jsonl converter web-sites might be used for this purpose. https://www.convertjson.com/json-to-jsonlines.htm <br>
	* a.1- Only the 3rd column is needed (the array column), first 2 columns are deleted. <br>
	* a.2- Upload the files to S3 again. <br>
	* a.3- Rerun the crawler.

* b- Convert .json files to .parquet files with an AWS Glue Job. This way Athena can read array types. <br>
	* b.1- Create a Glue Job called 'youtube_project_glue_job_json_to_parquet'. <br>
	* b.2- Use [glue_job_json_to_jsonl_converter.py](https://github.com/erensakarya/de-youtube-project/blob/main/json_to_jsonl_converters/glue_job/glue_job_json_to_jsonl_converter.py) pyspark code.<br>
 	This code reads json files, drops unnecessarry columns, explodes array type column to multiple columns and writes to s3 as parquet files with desired number of files with coalesce or repartition methods. <br>
  	* b.3- Run the job manually and rerun the crawler (My data is fix meaning that new data is not coming on a hourly or daily basis or not a streaming data so no scheduler is needed but a scheduler could be added to to the job or a trigger could be added to a Glue Workflow if this job would be a part of it).

* c- Convert .json files to .parquet files with an AWS Lambda Function by using python with aws-wrangler and pandas. <br>
	* c.1- Create a Lambda Function called 'youtube_project_lambda_function_json_to_parquet' <br>
	* c.2- Use [lambda_function.py](https://github.com/erensakarya/de-youtube-project/blob/main/json_to_jsonl_converters/lambda_function/lambda_function.py) python code. <br>
  	This code reads json files with aws-wrangler library, drops unnecessarry columns and renames remaining columns with pandas library and writes to s3 as .parquet files.
	* c.3- Deploy the function with .yaml and .py files by using  [deploy_lambda_function.sh](https://github.com/erensakarya/de-youtube-project/blob/main/json_to_jsonl_converters/deploy_lambda_function.sh) or you can .zip the folder and upload from AWS Console directly.
  	* c.4- Run the function by invoking test event. The test event example can be found here [event_example.json](https://github.com/erensakarya/de-youtube-project/blob/main/json_to_jsonl_converters/lambda_function/event_example.json)
  	* Add yaml and aws- wrangler layers (pyyaml:1, AWSDataWrangler-Python38:4). <br>
 	* You can use pyyaml layer for yaml lib and this arn "arn:aws:lambda:eu-west-1:336392948345:layer:AWSDataWrangler-Python38:4" for aws-wrangler lib.
   	aws-wrangler packages can be found in this url: https://aws-sdk-pandas.readthedocs.io/en/2.15.0/layers.html <br>
	* Change RAM limit to 512 MB (default:128MB) and timoeout to 1 minutes (default 3 seconds). <br>
	* This is one shot run but if the data would be coming with a fix time-interval then, we should have created a trigger either a time trigger with AWS Event Bridge or S3 	trigger.
 
 #### 2- Go to AWS Athena Query Editor and run below queries to have a look.
   ```sql
   SELECT * FROM youtube_project.raw_statistics WHERE region='de' limit 5;

   SELECT * FROM youtube_project.raw_statistics_reference_data limit 5;

   SELECT * FROM youtube_project.raw_statistics AS rs
   INNER JOIN youtube_project.raw_statistics_reference_data_parquet AS rsrft
   ON rs.category_id = cast(rsrft.id as int);
   ```
#### 3- Preprocessing should be applied to raw_statistics data, since in a normal scenario we wouldn't want to keep all the raw.
* A Glue job will be used for this purpose. <br>
	* Create a Glue Job called 'youtube_project_glue_job_raw_statistics_cleaner'. <br>
	* Use [glue_job_raw_statistics_cleaner.py](https://github.com/erensakarya/de-youtube-project/blob/main/glue_job_raw_statistics_cleaner/glue_job_raw_statistics_cleaner.py) pyspark code.<br>
 	This code reads .csv files, uses input_file_name method to get partition as a column, drop rows that have a null value and writes to s3 as parquet files with region partition. <br>
  	* Run the job manually, create a new crawler called "youtube_project_raw_statistics_cleansed_crawler" for cleansed_raw_statistics data and run the crawler.
  	* Glue job_bookmark can be enabled if this ETL job is going to run in schedule.

#### 4- Create final daily analysis Glue job.
* A Glue job will be used for this purpose. <br>
 	* Create a Glue Job called 'youtube_project_glue_job_final_daily_analysis'. <br>
 	* Use [glue_job_final_daily_analysis.py](https://github.com/erensakarya/de-youtube-project/blob/main/glue_job_raw_statistics_cleaner/glue_job_raw_statistics_cleaner.py) pyspark code.<br>
	This code reads raw_statistics_cleansed and raw_statistics_reference_data_parquet datas, inner joins them category_id and id and writes to s3 as parquet files with process_date partition daily <br>
 	* Create a daily schedule.
  	* Run the job manually, create a new crawler called "youtube_project_final_daily_analysis_crawler" for final_daily_analysis data and run the crawler.
  	* Glue job_bookmark can be enabled if this ETL job is going to run in schedule.
