import datetime
import pendulum
from function.extract_load_surveys import extract_all_surveys
from function.extract_load_responses import extract_all_responses
from function.extract_load_questions import extract_all_questions
from function.extract_load_answers import extract_all_answers
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator


with DAG(
        dag_id='decipher-pipeline',
        schedule_interval='0 0 * * *',
        description='Perform end to end using airflow',
        start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=15),
        tags=['decipher-pipeline'],
) as dag:
    aws_conn_id = "aws_conn"
    s3_bucket =  "{{ var.value.S3_BUCKET }}"  
    token_api = "{{ var.value.token }}"
    data_interval_end = "{{ data_interval_end.format('YYYYMMDDHHmmss') }}" 

    snowflake_conn = "snowflake_conn"
    SURVEYS_SNOWFLAKE_TABLE = "SURVEYS_RAW"
    RESPONSES_SNOWFLAKE_TABLE = "RESPONSES_RAW"
    QUESTIONS_SNOWFLAKE_TABLE = "QUESTIONS_RAW"
    ANSWERS_SNOWFLAKE_TABLE = "ANSWERS_RAW"
    SNOWFLAKE_STAGE = "TVC_SURVEYS_STAGE"
    SNOWFLAKE_SCHEMA = "FORSTA_DEV"
    SNOWFLAKE_ROLE = "ACCOUNTADMIN"
    
    extract_survey_task = PythonOperator(
    task_id="extract_surveys",
    python_callable=extract_all_surveys,
    op_kwargs={
        "s3_conn_id": aws_conn_id,
        "s3_bucket": s3_bucket,
        "token_api": token_api,
        "data_interval_end": data_interval_end,
    }
)

    extract_responses_task = PythonOperator(
    task_id="extract_responses",
    python_callable=extract_all_responses,
    op_kwargs={
        "s3_conn_id": aws_conn_id,
        "s3_bucket": s3_bucket,
        "token_api": token_api,
        "data_interval_end": data_interval_end,
    }
)


    extract_questions_task = PythonOperator(
    task_id="extract_questions",
    python_callable=extract_all_questions,
    op_kwargs={
        "s3_conn_id": aws_conn_id,
        "s3_bucket": s3_bucket,
        "token_api": token_api,
        "data_interval_end": data_interval_end,
    }
)
    
    extract_answers_task = PythonOperator(
    task_id="extract_answers",
    python_callable=extract_all_answers,
    op_kwargs={
        "s3_conn_id": aws_conn_id,
        "s3_bucket": s3_bucket,
        "token_api": token_api,
        "data_interval_end": data_interval_end,
    }
)


    truncate_surveys_get_latest = SnowflakeOperator(
    task_id="truncate_surveys_get_latest",
    sql=f"truncate table {SURVEYS_SNOWFLAKE_TABLE}",
    snowflake_conn_id=snowflake_conn,
    #parameters={"id": 56},
)
    
    truncate_ques_get_latest = SnowflakeOperator(
    task_id="truncate_ques_get_latest",
    sql=f"truncate table {QUESTIONS_SNOWFLAKE_TABLE}",
    snowflake_conn_id=snowflake_conn,
    #parameters={"id": 56},
)


    truncate_ans_get_latest = SnowflakeOperator(
    task_id="truncate_ans_get_latest",
    sql=f"truncate table {ANSWERS_SNOWFLAKE_TABLE}",
    snowflake_conn_id=snowflake_conn,
    #parameters={"id": 56},
)


    copy_extract_surveys_table = S3ToSnowflakeOperator(
    task_id="copy_extract_surveys_table",
    snowflake_conn_id=snowflake_conn,
    s3_keys= ["{{ ti.xcom_pull(task_ids='extract_surveys') }}"],
    table=SURVEYS_SNOWFLAKE_TABLE,
    schema= SNOWFLAKE_SCHEMA,
    role = SNOWFLAKE_ROLE,
    stage = SNOWFLAKE_STAGE,
    file_format="(type = 'PARQUET')", 
    pattern=".*[.]parquet",
)
    
    copy_extract_responses_table = S3ToSnowflakeOperator(
    task_id="copy_extract_responses_table",
    snowflake_conn_id=snowflake_conn,
    s3_keys= ["{{ ti.xcom_pull(task_ids='extract_responses') }}"],
    table=RESPONSES_SNOWFLAKE_TABLE,
    schema= SNOWFLAKE_SCHEMA,
    role = SNOWFLAKE_ROLE,
    stage = SNOWFLAKE_STAGE,
    file_format="(type = 'PARQUET')", 
    pattern=".*[.]parquet",
)
    
    copy_extract_ques_table = S3ToSnowflakeOperator(
    task_id="copy_extract_ques_table",
    snowflake_conn_id=snowflake_conn,
    s3_keys= ["{{ ti.xcom_pull(task_ids='extract_questions') }}"],
    table= QUESTIONS_SNOWFLAKE_TABLE,
    schema= SNOWFLAKE_SCHEMA,
    role = SNOWFLAKE_ROLE,
    stage = SNOWFLAKE_STAGE,
    file_format="(type = 'PARQUET')", 
    pattern=".*[.]parquet",
)
    
    copy_extract_ans_table = S3ToSnowflakeOperator(
    task_id="copy_extract_ans_table",
    snowflake_conn_id=snowflake_conn,
    s3_keys= ["{{ ti.xcom_pull(task_ids='extract_answers') }}"],
    table= ANSWERS_SNOWFLAKE_TABLE,
    schema= SNOWFLAKE_SCHEMA,
    role = SNOWFLAKE_ROLE,
    stage = SNOWFLAKE_STAGE,
    file_format="(type = 'PARQUET')", 
    pattern=".*[.]parquet",
)
    
    dbt_sync = EcsRunTaskOperator(
    task_id="dbt_transform",
    aws_conn_id= aws_conn_id,
    cluster = "dbtcluster",
    launch_type = "EC2",
    task_definition = "dbttask1",
    region = "us-east-1",
    overrides = {},
    )


#[extract_ranking_task, extract_yellow_cards_task] >> dummy_task >> [copy_ranking_table, copy_yellow_cards_table]
    extract_survey_task >> extract_responses_task >> extract_questions_task >> extract_answers_task >> truncate_surveys_get_latest >> truncate_ques_get_latest >>truncate_ans_get_latest >> copy_extract_surveys_table>>copy_extract_responses_table>>copy_extract_ques_table>>copy_extract_ans_table>>dbt_sync
    #extract_responses_task >> snowflake_op_with_params >> copy_extract_responses_table