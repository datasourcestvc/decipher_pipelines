from decipher.beacon import api
import logging
import json
import pandas as pd
import tempfile
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from function.Decipher import decipher_interface
logger = logging.getLogger()
logger.setLevel(logging.INFO)
import logging, os, time
from datetime import datetime




def extract_all_answers(s3_conn_id,s3_bucket,token_api,data_interval_end,scope="all")->list:
    """
    downlaod data from API key 
    push data to S3 bucket
    """
    try:
        logging.info('extracting data')
        decipher = decipher_interface(token_api)
        surveys = decipher.get_surveys(scope="all")

        res = None
        for survey in surveys:
            survey_id = survey["path"]
            try:
                response = decipher.get_survey_questions(survey_id)
            except Exception as e:
                print(e)
                response = None
        
            if response:
                df = decipher.create_dataframe(response)
                df['survey_id'] = survey_id
                df['current_ts'] = datetime.now()
                df['question_id'] = df['label']

                if not df is None:
                    if res is None:
                        res = df
                    else:
                        res = pd.concat([res, df], axis = 0)
           
        df = res
        df.drop(df.columns.difference(['current_ts', 'question_id','survey_id', 'type', 'values']), axis=1, inplace=True)
        
        df = df.explode("values")
       
        values_df = df['values'].apply(lambda x: pd.Series(x, dtype="object"))
        
        df = pd.concat([df, values_df.add_prefix('values_')], axis=1)
      


        df.dropna(how='all', axis=1, inplace=True)

        df = df.astype({'values_value': 'Int64'})
        
        df['answer_id'] = df['values_value']
        logging.info('successfully downloaded data, cols:')
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_parquet_file:
            df.to_parquet(temp_parquet_file.name, engine='pyarrow')
            temp_parquet_file_path = temp_parquet_file.name

        expected_new_key = f'answers/{data_interval_end}/{data_interval_end}.parquet'
        logging.info(f'Uploading file to S3 with key: {expected_new_key}')
        s3_hook = S3Hook(aws_conn_id=s3_conn_id)
        s3_hook.load_file(filename=temp_parquet_file_path, bucket_name=s3_bucket, key=expected_new_key, replace=True)
        os.remove(temp_parquet_file_path)
        return expected_new_key
    except Exception as e:
        logging.error(f'failed to load data into s3 {e}')
        raise