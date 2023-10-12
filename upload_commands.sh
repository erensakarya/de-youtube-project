# upload raw_statistics_reference_data .json files to s3 with a single thread.
aws s3 cp . s3://youtube-project/development/youtube/raw_statistics_reference_data/ --recursive --exclude "*" --include "*.json"

# upload raw_statistics .csv files with region partion by using nohup for multiple uploading.
nohup aws s3 cp CAvideos.csv s3://youtube-project/development/youtube/raw_statistics/region=ca/ &
nohup aws s3 cp DEvideos.csv s3://youtube-project/development/youtube/raw_statistics/region=de/ &
nohup aws s3 cp FRvideos.csv s3://youtube-project/development/youtube/raw_statistics/region=fr/ &
nohup aws s3 cp GBvideos.csv s3://youtube-project/development/youtube/raw_statistics/region=gb/ &
nohup aws s3 cp INvideos.csv s3://youtube-project/development/youtube/raw_statistics/region=in/ &
nohup aws s3 cp JPvideos.csv s3://youtube-project/development/youtube/raw_statistics/region=jp/ &
nohup aws s3 cp KRvideos.csv s3://youtube-project/development/youtube/raw_statistics/region=kr/ &
nohup aws s3 cp MXvideos.csv s3://youtube-project/development/youtube/raw_statistics/region=mx/ &
nohup aws s3 cp RUvideos.csv s3://youtube-project/development/youtube/raw_statistics/region=ru/ &
nohup aws s3 cp USvideos.csv s3://youtube-project/development/youtube/raw_statistics/region=us/ &
