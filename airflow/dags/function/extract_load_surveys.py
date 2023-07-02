from decipher.beacon import api
import logging
import json
import pandas as pd
import tempfile
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from function.Decipher import decipher_interface
# Set logging to debug to view logging messages in terminal
#logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
import logging, os, time
from datetime import datetime



# def get_all_surveys(token, scope="all"):
#     decipher = decipher_interface(token)
#     response = api.get(f"rh/companies/{scope}/surveys")
#     surveys = decipher.get_surveys(scope)
        
#     df = decipher.create_dataframe(surveys)
#     #df = pd.json_normalize(surveys)
        
#     df['survey_id'] = df['path']
#     df['current_ts'] = datetime.now()
 
#     return df

def extract_all_surveys(s3_conn_id,s3_bucket,token_api,data_interval_end,scope="all")->list:
    """
    downlaod data from API key 
    push data to S3 bucket
    """
    try:
        logging.info('extracting data')
        decipher = decipher_interface(token_api)
        response = api.get(f"rh/companies/{scope}/surveys")
        surveys = decipher.get_surveys(scope)
            
        df = decipher.create_dataframe(surveys)
            
        df['survey_id'] = df['path']
        df['current_ts'] = datetime.now()
        logging.info('successfully downloaded data, cols:')
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_parquet_file:
            df.to_parquet(temp_parquet_file.name, engine='pyarrow')
            temp_parquet_file_path = temp_parquet_file.name

        expected_new_key = f'surveys/{data_interval_end}/{data_interval_end}.parquet'
        logging.info(f'Uploading file to S3 with key: {expected_new_key}')
        s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        s3_hook.load_file(filename=temp_parquet_file_path, bucket_name=s3_bucket, key=expected_new_key, replace=True)
        os.remove(temp_parquet_file_path)
        return expected_new_key
    except Exception as e:
        logging.error(f'failed to load data into s3 {e}')
        raise




'''def extract_all_surveys(s3_conn_id,s3_bucket,token_api,data_interval_end,scope="all")->list:
    """
    downlaod data from API key 
    push data to S3 bucket
    """
    # headers = {"X-RapidAPI-Key" : api_key}
    # params = {"season":premier_league_season, "league":league_id}
    try:
        logging.info('extracting data')
        decipher = decipher_interface(token_api)
        response = api.get(f"rh/companies/{scope}/surveys")
        surveys = decipher.get_surveys(scope)
            
        df = decipher.create_dataframe(surveys)
        #df = pd.json_normalize(surveys)
            
        df['survey_id'] = df['path']
        df['current_ts'] = datetime.now()
        logging.info('successfully downloaded data, cols:')
        # load result back to S3
        
        # with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_csv_file:
        #     df.to_csv(temp_csv_file, index=False, header=True)
        #     temp_csv_file_path = temp_csv_file.name

        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_csv_file:
            df.to_csv(temp_csv_file, index=False, header=True)
            temp_csv_file_path = temp_csv_file.name

        #expected_new_key = f'surveys/{data_interval_end}/{data_interval_end}.csv'
        expected_new_key = f'surveys/{data_interval_end}/{data_interval_end}.csv'
        logging.info(f'Uploading file to S3 with key: {expected_new_key}')
        s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        s3_hook.load_file(filename=temp_csv_file_path, bucket_name=s3_bucket, key=expected_new_key, replace=True)
        os.remove(temp_csv_file_path)
        return expected_new_key
        #return f's3://{s3_bucket}/{expected_new_key}'
    except Exception as e:
            logging.error(f'failed to load data into s3 {e}')
            raise 
'''
   
