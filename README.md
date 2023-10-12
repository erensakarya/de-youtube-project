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
### 1- Creating Glue Tables with Glue Crawlers to be able to run queries with AWS Athena.
Download the Youtube data and upload to "youtube-project" S3 bucket with commands which are in [upload_commands.sh](https://github.com/erensakarya/de-youtube-project/blob/main/upload_commands.sh) file.<br>
Create glue crawlers for each data so tables can be created in Glue Catalog like below;
* youtube_project_raw_statistics_crawler for raw_statistics data and raw_statistics table with region partition.
* youtube_project_raw_statistics_reference_data_crawler for raw_statistics_reference_data data and raw_statistics_reference_data table with no partition.

Both tables will be created in the Glue Catalog but while 1st table can be queried successfully with AWS Athena (since background files are .csv), 2nd table can't be queried with Athena. Because the background .json datas are not .jsonl (new line delimited json files), instead they are old fashioned multiline .json files. Therefore, Athena can't handle these type .jsons.<br>
To handle this sitution, we have 3 options like below;
* 1- Convert .json files to .jsonl files. json to jsonl converter web-sites might be used for this purpose. (https://www.convertjson.com/json-to-jsonlines.htm)
	1.1- Only the 3rd column is needed (the array column), first 2 columns are deleted.
	1.2- Upload the files to S3 again.
	1.3- Rerun the crawler.
* 2- Convert .json files to .parquet files with an AWS Glue Job. This way Athena can read array types.
	2.1- Create a Glue Job called 'youtube_project_glue_job_json_to_parquet'.
	2.2- Use Write your spark code (first read json files, drop unnecessarry columns, explode array type column to multiple columns and write to s3 as parquet files with desired number of files with sparkGroupBy parameter.


