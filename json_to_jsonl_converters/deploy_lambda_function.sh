#!/bin/bash
FOLDER_NAME="lambda_function"

zip -r $FOLDER_NAME.zip ./ -x ".DS_Store" "deploy.sh" "event_example.json" "delete_custom_campaign_with_sqs.zip"

CREATE_RESULT=$(aws lambda create-function --function-name $FOLDER_NAME \
  --zip-file fileb://$FOLDER_NAME.zip --handler lambda_function.lambda_handler --runtime python3.8 \
  --timeout 60 --memory-size 512 \
  --layers 'arn:aws:lambda:eu-west-1:669179381143:layer:pyyaml:1' 'arn:aws:lambda:eu-west-1:336392948345:layer:AWSDataWrangler-Python38:4' \
  --role <iam_role_arn>)


echo CREATE_RESULT | grep "error"
if [ $? == 1 ]; then
  UPDATE_RESULT=$(aws lambda update-function-code \
    --function-name $FOLDER_NAME \
    --zip-file fileb://$FOLDER_NAME.zip)

  echo "**** function code updated!!!! ------>"
  echo $UPDATE_RESULT
else
  echo "**** function created!!!! ------>"
  echo $CREATE_RESULT
fi
