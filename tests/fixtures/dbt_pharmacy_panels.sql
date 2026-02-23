{{
       config(
              materialized='table',
              )
}}
/*
Creates the panel spans for pharmacy based on appointments,
Creates spans for lipid management and for MTM
*/
WITH cp AS (
       SELECT id AS clinical_program_id
            , key AS clinical_program_key
            , name AS clinical_program_name
         FROM {{ source('orinoco', 'clinical_programs') }}
        WHERE clinical_program_key LIKE 'mtm-____'
              AND TRY_TO_NUMBER(RIGHT(clinical_program_key, 4)) >= 2023 -- MTM and MAGII have new clinical programs each year
)
, mcp AS (
       SELECT member_id
            , id AS member_clinical_program_id
            , clinical_program_id
         FROM {{ source('orinoco', 'member_clinical_programs') }} AS mcp
        WHERE EXISTS (
              SELECT 1
                FROM cp
               WHERE mcp.clinical_program_id = cp.clinical_program_id
              )
              AND status != 'DELETED'
)
, clinical_activities AS (
       SELECT id
            , member_clinical_program_id
            , name
         FROM {{ source('orinoco', 'clinical_activities') }} AS clinical_activities
        WHERE clinical_activities.status NOT IN (
                     'CANCELLED'
                   , 'DELETED'
              )
)
, mtm_appts AS (
       SELECT appts.id AS unique_id
            , appts.member_id
            , YEAR(scheduled_date) AS panel_year
            , scheduled_date
            , scheduled_with_user_id AS assigned_user_id
            , scheduled_with AS assigned_user_name
            , COALESCE(
                     cp.clinical_program_id
                   , cp_no_mcp.clinical_program_id
              ) AS clinical_program_id
            , COALESCE(
                     cp.clinical_program_key
                   , cp_no_mcp.clinical_program_key
              ) AS clinical_program_key
            , COALESCE(
                     cp.clinical_program_name
                   , cp_no_mcp.clinical_program_name
              ) AS clinical_program_name
            , status
            , 'PHARMACY_APPOINTMENT' AS panel_start_reason
            , UPPER(REPLACE(appointment_type, '- ', '_')) AS panel_start_sub_reason
         FROM {{ ref('dmg_appointment_fact') }} AS appts
       /*
         Need to determine which clinical program gets the panel.
         Try in this order: MCP on Appts, MCP on clinical activities, mtm based on appt scheduled date year
         */
         LEFT JOIN clinical_activities
           ON appts.clinical_activity_id = clinical_activities.id
         LEFT JOIN mcp
           ON COALESCE(
                     appts.member_clinical_program_id
                   , clinical_activities.member_clinical_program_id
              ) = mcp.member_clinical_program_id
         LEFT JOIN cp
           ON mcp.clinical_program_id = cp.clinical_program_id
         LEFT JOIN cp AS cp_no_mcp
           ON CONCAT('mtm-', YEAR(appts.scheduled_date)) = cp_no_mcp.clinical_program_key
        WHERE scheduled_date BETWEEN '2023-01-01' AND CURRENT_DATE()
              AND status IN ('SUMMARIZED', 'COMPLETED')
              AND appointment_type_key IN (
                            'dhp-cmr'
                          , 'medication-management'
                     )
       -- Incase someone somehow gets more than one appt in a day, take latest if so.
       QUALIFY ROW_NUMBER() OVER (
               PARTITION BY appts.member_id, scheduled_date
                   ORDER BY scheduled_time DESC
              ) = 1
)
, member_info AS (
       SELECT member_id
            , effective_month
            , effective_month_end
            , death_date
         FROM {{ ref('member_info_monthly') }} AS mem
        WHERE EXISTS (
              SELECT 1
                FROM mtm_appts
               WHERE mem.member_id = mtm_appts.member_id
              )
)
/* Incase we end up needing to reassign people in these panels */
, reassign AS (
       SELECT unique_id
            , member_id
            , 'REASSIGNED' AS panel_start_reason
            , reason_for_reassignment AS panel_start_sub_reason
            , assign_to_email
            , YEAR(reassignment_date) AS panel_year
            , reassignment_date AS panel_start_date
            , clinical_program_key
         FROM {{ ref('reassign_cav_panels_history') }} AS reassign
        WHERE EXISTS (
              SELECT 1
                FROM cp
               WHERE reassign.clinical_program_key = cp.clinical_program_key
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
                   , reassign.member_id
                   , panel_start_reason
                   , panel_start_sub_reason
                   , panel_year
                   , panel_start_date
                   , assigned_user_id
                   , assigned_user_name
                   , cp.clinical_program_id
                   , cp.clinical_program_key
                   , cp.clinical_program_name
                   , 2 AS order_dupes
                FROM reassign
                JOIN cp
                  ON reassign.clinical_program_key = cp.clinical_program_key
                JOIN users
                  ON reassign.assign_to_email = users.email
               UNION ALL
              SELECT unique_id
                   , member_id
                   , panel_start_reason
                   , panel_start_sub_reason
                   , panel_year
                   , scheduled_date AS panel_start_date
                   , assigned_user_id
                   , assigned_user_name
                   , clinical_program_id
                   , clinical_program_key
                   , clinical_program_name
                   , 1 AS order_dupes
                FROM mtm_appts
       )
       QUALIFY ROW_NUMBER() OVER (
               PARTITION BY member_id, panel_start_date
                   ORDER BY order_dupes
              ) = 1
)
, find_end_dates AS (
       SELECT initial_spans.*
            , effective_month
            , effective_month_end
            , IFF(
                     death_date BETWEEN panel_start_date AND temp_end_date
                   , death_date
                   , NULL
              ) AS death_date
            , LEAD(effective_month) OVER (
               PARTITION BY initial_spans.member_id
                          , panel_start_date
                          , panel_year
                   ORDER BY effective_month
              ) AS next_month
            , CONDITIONAL_TRUE_EVENT(
                     DATEDIFF('month', effective_month, next_month) > 1
              ) OVER (
               PARTITION BY initial_spans.member_id
                          , panel_start_date
                          , panel_year
                   ORDER BY effective_month
              ) AS enrollment_grouper
         FROM (
              SELECT unique_id
                   , member_id
                   , panel_start_reason
                   , panel_start_sub_reason
                   , panel_start_date
                   , panel_year
                   , IFF(
                            panel_start_reason = 'REASSIGNED'
                          , LAG(panel_start_date) OVER (
                             PARTITION BY member_id
                                 ORDER BY IFF(
                                                 panel_start_reason = 'REASSIGNED'
                                               , NULL
                                               , panel_start_date
                                          ) NULLS LAST
                            )
                          , panel_start_date
                     ) AS last_non_reassignment_start_date
                   , LEAD(
                            panel_start_date - 1
                          , 1
                          , LEAST(
                                   DATE_FROM_PARTS(panel_year, 12, 31)
                                 , CURRENT_DATE()
                            )
                     ) OVER (
                      PARTITION BY member_id, panel_year
                          ORDER BY panel_start_date
                     ) AS temp_end_date
                   , assigned_user_id
                   , assigned_user_name
                   , clinical_program_id
                   , clinical_program_key
                   , clinical_program_name
                FROM combined_spans
       ) AS initial_spans
         JOIN member_info
           ON initial_spans.member_id = member_info.member_id
              AND effective_month BETWEEN DATE_TRUNC('month', panel_start_date) AND temp_end_date
)
, true_spans AS (
       SELECT unique_id
            , ANY_VALUE(member_id) AS member_id
            , ANY_VALUE(panel_start_reason) AS panel_start_reason
            , ANY_VALUE(panel_start_sub_reason) AS panel_start_sub_reason
            , ANY_VALUE(assigned_user_id) AS assigned_user_id
            , ANY_VALUE(assigned_user_name) AS assigned_user_name
            , ANY_VALUE(clinical_program_id) AS clinical_program_id
            , ANY_VALUE(clinical_program_key) AS clinical_program_key
            , ANY_VALUE(clinical_program_name) AS clinical_program_name
            , ANY_VALUE(last_non_reassignment_start_date) AS last_non_reassignment_start_date
            , GREATEST(
                     MIN(panel_start_date)
                   , MIN(effective_month)
              ) AS panel_start_date
            , least_ignore_nulls(
                     MIN(temp_end_date)
                   , MAX(effective_month_end)
                   , MAX(death_date)
                   , CURRENT_DATE()
              ) AS panel_end_date
         FROM find_end_dates
        GROUP BY unique_id
            , enrollment_grouper
)
, mtm_spans AS (
       SELECT MD5(CONCAT(panel_start_date, unique_id)) AS unique_id
            , true_spans.member_id
            , true_spans.unique_id AS panel_id
            , panel_start_reason
            , panel_start_sub_reason
            , panel_start_date
            , panel_end_date
            , assigned_user_id
            , assigned_user_name
            , NULL AS assigned_user_group_id
            , NULL AS assigned_user_group_name
            , COALESCE(assigned_user_id, assigned_user_group_id) AS assigned_id_coalesced
            , COALESCE(
                     assigned_user_name
                   , assigned_user_group_name
              ) AS assigned_name_coalesced
            , true_spans.clinical_program_id
            , clinical_program_key
            , clinical_program_name
            , member_clinical_program_id
            , 'ENROLLED' AS member_clinical_program_status
            , 'PHARMACY' AS panel_span_type
            , UPPER(SPLIT_PART(clinical_program_key, '-', 1)) AS panel_span_subtype
            , FALSE AS needs_panel_paused
            , last_non_reassignment_start_date
         FROM true_spans
         LEFT JOIN mcp
           ON true_spans.member_id = mcp.member_id
              AND true_spans.clinical_program_id = mcp.clinical_program_id
)
SELECT *
  FROM mtm_spans
