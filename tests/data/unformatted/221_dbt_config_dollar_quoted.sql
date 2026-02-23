-- disable-parser
{{
       config(
              materialized='incremental',
              transient=false,
              unique_key='event_id',
              meta={'final_schema': 'analytics'},
              incremental_strategy='merge',
              on_schema_change='sync_all_columns',
              full_refresh=false,
       )
}}
{% set app_comment_pattern = "'-- App Context'" %}
SELECT e.event_id AS event_id
     , e.event_type AS event_type
     , e.user_id AS user_id
     , e.created_at AS created_at
     , CASE
         WHEN e.source = 'api' THEN 'api'
         WHEN e.source = 'web' THEN 'web'
         ELSE 'other'
          END AS event_source
     , try_parse_json(regexp_substr(e.payload, $$/\*\s*({.*"app":.*})\s*\*/$$, 1, 1, 'ie')) as event_meta
     , e.duration_ms AS duration_ms
  FROM {{ source('app', 'events') }} AS e
  LEFT JOIN {{ ref('dim_users') }} AS u
    ON e.user_id = u.user_id
{% if is_incremental() %}
 WHERE e.created_at > (
       SELECT DATEADD(DAY, -2, MAX(created_at))
         FROM {{ this }}
       )
{% endif %}
)))))__SQLFMT_OUTPUT__(((((
-- disable-parser
{{
       config(
              materialized='incremental',
              transient=false,
              unique_key='event_id',
              meta={'final_schema': 'analytics'},
              incremental_strategy='merge',
              on_schema_change='sync_all_columns',
              full_refresh=false,
       )
}}
{% set app_comment_pattern = "'-- App Context'" %}
select
    e.event_id as event_id,
    e.event_type as event_type,
    e.user_id as user_id,
    e.created_at as created_at,
    case
        when e.source = 'api' then 'api' when e.source = 'web' then 'web' else 'other'
    end as event_source,
    try_parse_json(
        regexp_substr(e.payload, $$/\*\s*({.*"app":.*})\s*\*/$$, 1, 1, 'ie')
    ) as event_meta,
    e.duration_ms as duration_ms
from {{ source("app", "events") }} as e
left join {{ ref("dim_users") }} as u on e.user_id = u.user_id
{% if is_incremental() %}
    where e.created_at > (select dateadd(day, -2, max(created_at)) from {{ this }})
{% endif %}
