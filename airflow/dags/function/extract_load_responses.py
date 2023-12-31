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



'''def extract_all_responses(s3_conn_id,s3_bucket,token_api,data_interval_end,scope="all",start_date='2022-01-01T00:00Z',end_date='2023-07-13T00:00Z')->list:
    """
    downlaod data from API key 
    push data to S3 bucket
    """
    try:
        logging.info('extracting data')
        decipher = decipher_interface(token_api)
        surveys = decipher.get_surveys(scope="all")

        res = pd.DataFrame()
        for survey in surveys:
            sid = survey["path"]
            survey_data = decipher.get_survey_data(sid, start=start_date, end=end_date)
            
            if survey_data:
                df = decipher.create_dataframe(survey_data)
                if df is None or df.empty:
                    continue
                
                df['survey_id'] = sid
                df['current_ts'] = datetime.now()
                
                df = df.melt(id_vars=['uuid', 'survey_id', 'start_date', 'current_ts'],
                            var_name='question_id',
                            value_name='answer_id')
                
                df = df.dropna(subset=['answer_id'])
                
                res = pd.concat([res, df], ignore_index=True)
        logging.info('successfully downloaded data, cols:')
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_parquet_file:
            res.to_parquet(temp_parquet_file.name, engine='pyarrow')
            temp_parquet_file_path = temp_parquet_file.name

        expected_new_key = f'responses/{data_interval_end}/{data_interval_end}.parquet'
        logging.info(f'Uploading file to S3 with key: {expected_new_key}')
        s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        s3_hook.load_file(filename=temp_parquet_file_path, bucket_name=s3_bucket, key=expected_new_key, replace=True)
        os.remove(temp_parquet_file_path)
        return expected_new_key
    except Exception as e:
        logging.error(f'failed to load data into s3 {e}')
        raise'''



from datetime import datetime, timedelta
import pandas as pd

def extract_all_responses(s3_conn_id, s3_bucket, token_api, data_interval_end, scope="all",
                          start_date=None, end_date=None):
    """
    Download data from API key and push data to S3 bucket.
    """

    if start_date is None:
        start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%dT00:00Z')
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%dT00:00Z')

    try:
        logging.info('extracting data')
        decipher = decipher_interface(token_api)
        surveys = decipher.get_surveys(scope="all")

        res = pd.DataFrame()
        for survey in surveys:
            sid = survey["path"]
            survey_data = decipher.get_survey_data(sid, start=start_date, end=end_date)

            if survey_data:
                df = decipher.create_dataframe(survey_data)
                if df is None or df.empty:
                    continue

                df['survey_id'] = sid
                df['current_ts'] = datetime.now()

                df = df.melt(id_vars=['uuid', 'survey_id', 'start_date', 'current_ts'],
                             var_name='question_id',
                             value_name='answer_id')

                df = df.dropna(subset=['answer_id'])

                res = pd.concat([res, df], ignore_index=True)
        logging.info('successfully downloaded data, cols:')

        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_parquet_file:
            res.to_parquet(temp_parquet_file.name, engine='pyarrow')
            temp_parquet_file_path = temp_parquet_file.name

        expected_new_key = f'responses/{data_interval_end}/{data_interval_end}.parquet'
        logging.info(f'Uploading file to S3 with key: {expected_new_key}')
        s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        s3_hook.load_file(filename=temp_parquet_file_path, bucket_name=s3_bucket, key=expected_new_key, replace=True)
        os.remove(temp_parquet_file_path)
        return expected_new_key
    except Exception as e:
        logging.error(f'failed to load data into s3 {e}')
        raise


