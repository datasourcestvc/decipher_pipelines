{{
    config(
        materialized='table',
        database='TVC_SURVEYS_PROD',
        schema='PROD'
    )
}}

select
  src:survey_id::VARCHAR as survey_id,
  src:question_id::VARCHAR as question_id,
  src:answer_id::BIGINT as answer_id, 
  src:values_value::BIGINT as values_value,
  src:values_title::VARCHAR as values_title,
  src:values_label::VARCHAR as values_label,
  src:"type"::VARCHAR as "type",
  src:"values"::VARCHAR as "values",
  TO_TIMESTAMP(src:current_ts::STRING) AS Current_TS 
  FROM {{ source('FORSTA_DEV', 'ANSWERS_RAW') }}