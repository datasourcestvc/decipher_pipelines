{{
    config(
        materialized='table',
        database='TVC_SURVEYS_PROD',
        schema='PROD'
    )
}}

select
  src:survey_id::VARCHAR as survey_id,
  src:uuid::VARCHAR as uuid,
  src:start_date::VARCHAR as start_date, 
  src:question_id::VARCHAR as question_id,
  src:answer_id::VARCHAR as answer_id,
  TO_TIMESTAMP(src:current_ts::STRING) AS Current_TS 
  FROM {{ source('FORSTA_DEV', 'RESPONSES_RAW') }}