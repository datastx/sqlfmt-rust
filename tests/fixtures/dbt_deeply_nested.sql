{{
       config(
              materialized='table',
              )
}}
/*
Creates tracking spans for items based on events,
Creates spans for category alpha and for category beta
*/
WITH ct AS (
       SELECT id AS category_id
            , key AS category_key
            , name AS category_name
         FROM {{ source('warehouse', 'categories') }}
        WHERE category_key LIKE 'grp-____'
              AND TRY_TO_NUMBER(RIGHT(category_key, 4)) >= 2023 -- Groups have new categories each year
)
, ec AS (
       SELECT entity_id
            , id AS entity_category_id
            , category_id
         FROM {{ source('warehouse', 'entity_categories') }} AS ec
        WHERE EXISTS (
              SELECT 1
                FROM ct
               WHERE ec.category_id = ct.category_id
              )
              AND status != 'DELETED'
)
, tasks AS (
       SELECT id
            , entity_category_id
            , name
         FROM {{ source('warehouse', 'tasks') }} AS tasks
        WHERE tasks.status NOT IN (
                     'CANCELLED'
                   , 'DELETED'
              )
)
, grp_events AS (
       SELECT events.id AS unique_id
            , events.entity_id
            , YEAR(event_date) AS span_year
            , event_date
            , assigned_to_user_id AS assigned_user_id
            , assigned_to AS assigned_user_name
            , COALESCE(
                     ct.category_id
                   , ct_no_ec.category_id
              ) AS category_id
            , COALESCE(
                     ct.category_key
                   , ct_no_ec.category_key
              ) AS category_key
            , COALESCE(
                     ct.category_name
                   , ct_no_ec.category_name
              ) AS category_name
            , status
            , 'ITEM_EVENT' AS span_start_reason
            , UPPER(REPLACE(event_type, '- ', '_')) AS span_start_sub_reason
         FROM {{ ref('dim_event_fact') }} AS events
       /*
         Need to determine which category gets the span.
         Try in this order: EC on events, EC on tasks, group based on event date year
         */
         LEFT JOIN tasks
           ON events.task_id = tasks.id
         LEFT JOIN ec
           ON COALESCE(
                     events.entity_category_id
                   , tasks.entity_category_id
              ) = ec.entity_category_id
         LEFT JOIN ct
           ON ec.category_id = ct.category_id
         LEFT JOIN ct AS ct_no_ec
           ON CONCAT('grp-', YEAR(events.event_date)) = ct_no_ec.category_key
        WHERE event_date BETWEEN '2023-01-01' AND CURRENT_DATE()
              AND status IN ('RESOLVED', 'COMPLETED')
              AND event_type_key IN (
                            'svc-review'
                          , 'item-management'
                     )
       -- In case someone gets more than one event in a day, take latest if so.
       QUALIFY ROW_NUMBER() OVER (
               PARTITION BY events.entity_id, event_date
                   ORDER BY event_time DESC
              ) = 1
)
, entity_info AS (
       SELECT entity_id
            , effective_month
            , effective_month_end
            , termination_date
         FROM {{ ref('entity_info_monthly') }} AS ent
        WHERE EXISTS (
              SELECT 1
                FROM grp_events
               WHERE ent.entity_id = grp_events.entity_id
              )
)
/* In case we end up needing to reassign items in these spans */
, reassign AS (
       SELECT unique_id
            , entity_id
            , 'REASSIGNED' AS span_start_reason
            , reason_for_reassignment AS span_start_sub_reason
            , assign_to_email
            , YEAR(reassignment_date) AS span_year
            , reassignment_date AS span_start_date
            , category_key
         FROM {{ ref('reassign_item_spans_history') }} AS reassign
        WHERE EXISTS (
              SELECT 1
                FROM ct
               WHERE reassign.category_key = ct.category_key
              )
)
, users AS (
       SELECT id AS assigned_user_id
            , email
            , full_name AS assigned_user_name
         FROM {{ ref('users') }}
)
, combined_spans AS (
       SELECT *
         FROM (
              SELECT unique_id
                   , reassign.entity_id
                   , span_start_reason
                   , span_start_sub_reason
                   , span_year
                   , span_start_date
                   , assigned_user_id
                   , assigned_user_name
                   , ct.category_id
                   , ct.category_key
                   , ct.category_name
                   , 2 AS order_dupes
                FROM reassign
                JOIN ct
                  ON reassign.category_key = ct.category_key
                JOIN users
                  ON reassign.assign_to_email = users.email
               UNION ALL
              SELECT unique_id
                   , entity_id
                   , span_start_reason
                   , span_start_sub_reason
                   , span_year
                   , event_date AS span_start_date
                   , assigned_user_id
                   , assigned_user_name
                   , category_id
                   , category_key
                   , category_name
                   , 1 AS order_dupes
                FROM grp_events
       )
       QUALIFY ROW_NUMBER() OVER (
               PARTITION BY entity_id, span_start_date
                   ORDER BY order_dupes
              ) = 1
)
, find_end_dates AS (
       SELECT initial_spans.*
            , effective_month
            , effective_month_end
            , IFF(
                     termination_date BETWEEN span_start_date AND temp_end_date
                   , termination_date
                   , NULL
              ) AS termination_date
            , LEAD(effective_month) OVER (
               PARTITION BY initial_spans.entity_id
                          , span_start_date
                          , span_year
                   ORDER BY effective_month
              ) AS next_month
            , CONDITIONAL_TRUE_EVENT(
                     DATEDIFF('month', effective_month, next_month) > 1
              ) OVER (
               PARTITION BY initial_spans.entity_id
                          , span_start_date
                          , span_year
                   ORDER BY effective_month
              ) AS enrollment_grouper
         FROM (
              SELECT unique_id
                   , entity_id
                   , span_start_reason
                   , span_start_sub_reason
                   , span_start_date
                   , span_year
                   , IFF(
                            span_start_reason = 'REASSIGNED'
                          , LAG(span_start_date) OVER (
                             PARTITION BY entity_id
                                 ORDER BY IFF(
                                                 span_start_reason = 'REASSIGNED'
                                               , NULL
                                               , span_start_date
                                          ) NULLS LAST
                            )
                          , span_start_date
                     ) AS last_non_reassignment_start_date
                   , LEAD(
                            span_start_date - 1
                          , 1
                          , LEAST(
                                   DATE_FROM_PARTS(span_year, 12, 31)
                                 , CURRENT_DATE()
                            )
                     ) OVER (
                      PARTITION BY entity_id, span_year
                          ORDER BY span_start_date
                     ) AS temp_end_date
                   , assigned_user_id
                   , assigned_user_name
                   , category_id
                   , category_key
                   , category_name
                FROM combined_spans
       ) AS initial_spans
         JOIN entity_info
           ON initial_spans.entity_id = entity_info.entity_id
              AND effective_month BETWEEN DATE_TRUNC('month', span_start_date) AND temp_end_date
)
, true_spans AS (
       SELECT unique_id
            , ANY_VALUE(entity_id) AS entity_id
            , ANY_VALUE(span_start_reason) AS span_start_reason
            , ANY_VALUE(span_start_sub_reason) AS span_start_sub_reason
            , ANY_VALUE(assigned_user_id) AS assigned_user_id
            , ANY_VALUE(assigned_user_name) AS assigned_user_name
            , ANY_VALUE(category_id) AS category_id
            , ANY_VALUE(category_key) AS category_key
            , ANY_VALUE(category_name) AS category_name
            , ANY_VALUE(last_non_reassignment_start_date) AS last_non_reassignment_start_date
            , GREATEST(
                     MIN(span_start_date)
                   , MIN(effective_month)
              ) AS span_start_date
            , least_ignore_nulls(
                     MIN(temp_end_date)
                   , MAX(effective_month_end)
                   , MAX(termination_date)
                   , CURRENT_DATE()
              ) AS span_end_date
         FROM find_end_dates
        GROUP BY unique_id
            , enrollment_grouper
)
, final_spans AS (
       SELECT MD5(CONCAT(span_start_date, unique_id)) AS unique_id
            , true_spans.entity_id
            , true_spans.unique_id AS span_id
            , span_start_reason
            , span_start_sub_reason
            , span_start_date
            , span_end_date
            , assigned_user_id
            , assigned_user_name
            , NULL AS assigned_user_group_id
            , NULL AS assigned_user_group_name
            , COALESCE(assigned_user_id, assigned_user_group_id) AS assigned_id_coalesced
            , COALESCE(
                     assigned_user_name
                   , assigned_user_group_name
              ) AS assigned_name_coalesced
            , true_spans.category_id
            , category_key
            , category_name
            , entity_category_id
            , 'ENROLLED' AS entity_category_status
            , 'TRACKING' AS span_type
            , UPPER(SPLIT_PART(category_key, '-', 1)) AS span_subtype
            , FALSE AS needs_span_paused
            , last_non_reassignment_start_date
         FROM true_spans
         LEFT JOIN ec
           ON true_spans.entity_id = ec.entity_id
              AND true_spans.category_id = ec.category_id
)
SELECT *
  FROM final_spans
