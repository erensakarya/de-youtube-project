import awswrangler as wr
import pandas as pd
import yaml


def get_params():
    try:
        with open('params.yaml') as file:
            params = yaml.load(file, Loader=yaml.FullLoader)
        return params
    except Exception as e:
        print(e)
        print('There is no params.yaml file or the .yaml file is malformed!!!')
        raise e


def lambda_handler(event, context):
    # Get the params.
    params = get_params()
    # Get the object from the event and show its content type.
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    try:
        # Creating df from content
        df_raw = wr.s3.read_json(f's3://{bucket}/{key}')

        # Extract required columns:
        df_items = pd.json_normalize(df_raw['items'])

        # Rename columns
        df_items = df_items.rename(
            columns={"snippet.channelId": "channelId", "snippet.title": "title", "snippet.assignable": "assignable"})

        # Write to S3
        wr_response = wr.s3.to_parquet(
            df=df_items,
            dataset=True,
            path=params['s3_cleansed_layer'],
            mode=params['s3_write_mode']
        )

        return wr_response
    except Exception as e:
        print(e)
        print(f'Error getting object {key} from bucket {bucket}!!!')
        raise e
