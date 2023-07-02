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
  src:"value"::FLOAT as "value",
  src:"values"::VARCHAR as "values",
  src:qlabel::VARCHAR as qlabel,
  src:"type"::VARCHAR as "type",
  src:col::VARCHAR as col,
  src:"row"::VARCHAR as "row",
  src:label::VARCHAR as label,
  src:rowTitle::VARCHAR as rowTitle,
  src:title::VARCHAR as title,
  src:colTitle::VARCHAR as colTitle,
  src:qtitle::VARCHAR as qtitle,
  src:vgroup::VARCHAR as vgroup,
  TO_TIMESTAMP(src:current_ts::STRING) AS Current_TS 
  FROM {{ source('FORSTA_DEV', 'QUESTIONS_RAW') }}