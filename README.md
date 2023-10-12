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
I downloaded the Youtube data and uploaded to my "youtube-project" S3 bucket with commands which are in upload_commands.sh file.

