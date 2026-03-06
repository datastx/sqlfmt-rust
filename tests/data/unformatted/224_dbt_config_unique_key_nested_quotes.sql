{{
       config(
              materialized='incremental',
              incremental_strategy='append',
              on_schema_change='append_new_columns',
              unique_key='"report_date"||"IFNULL(group_id,\'\')||"line_of_business"||"category"||COALESCE("label",group_id)"'
       )
}}
SELECT current_timestamp AS etl_at
     , *
  FROM {{ ref('stg_daily_summary') }}
 WHERE group_id IS NOT NULL
)))))__SQLFMT_OUTPUT__(((((
{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        on_schema_change="append_new_columns",
        unique_key='"report_date"||"IFNULL(group_id,\'\')||"line_of_business"||"category"||COALESCE("label",group_id)"',
    )
}}
select
    current_timestamp as etl_at
    , *
from {{ ref("stg_daily_summary") }}
where group_id is not null
