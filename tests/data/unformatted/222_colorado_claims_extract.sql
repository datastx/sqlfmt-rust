-- disable-parser: if statement creates invalid sql
{{ config(
    materialized="table",
    meta={'final_schema': 'integrations'}
) }}
/*         COLORADO CLAIMS MEDICAL EXTRACT
           dbtmodel: colorado_all_payers_claim_medical.sql
           Purpose:
           - Pull all data from transform.colorado_all_payers_claim_stage and format them as per the Colorado APCD requirements
           - Remove the fields that are for internal purposes
           - DETAILS of REPORT in this link:
             -- https://civhc.org/wp-content/uploads/2024/12/Data-Submission-Guide-DSG-v-16-Final.pdf
*/
WITH claims AS (
       SELECT claims.*
         FROM {{ ref('colorado_all_payers_claim_stage') }} AS claims
 WHERE target_month = TRY_TO_TIMESTAMP('{{ var("data_anchor_month") }}')
)
, claims_header_amts AS (
       SELECT *
            , MC063::FLOAT + MC064::FLOAT + MC065::FLOAT + MC066::FLOAT + MC067::FLOAT AS amt_per_record
            , IFF(MC220 = 'Y', amt_per_record, 0.0) AS amt_vision
            , IFF(MC209 = 'Y', amt_per_record, 0.0) AS amt_dental
            , IFF(MC209 != 'Y' AND MC220 != 'Y', amt_per_record, 0.0) AS amt_other
         FROM claims
)
, claims_count AS (
       SELECT COUNT(*) AS claim_count
            , TO_CHAR(
                     TRY_TO_TIMESTAMP('{{ var("data_anchor_month") }}')
                   , 'YYYYMM'
              ) AS report_month
            , SUM(amt_vision) AS total_amt_vision
            , SUM(amt_dental) AS total_amt_dental
            , SUM(amt_other) AS total_other_amt
            , SUM(amt_per_record) AS total_amt
         FROM claims_header_amts
)
, mem_eligible AS (
       SELECT COUNT( DISTINCT
                       CASE WHEN ME152 = 'Y' THEN member_id ELSE NULL END
              ) AS cnt_mem_vision_eligible
            , COUNT( DISTINCT
                       CASE WHEN ME020 = 'Y' THEN member_id ELSE NULL END
              ) AS cnt_mem_dental_eligible
            , COUNT( DISTINCT
                       CASE
                       WHEN ME018 = 'Y' OR ME123 = 'Y' THEN member_id
                       ELSE NULL
                        END
              ) AS cnt_all_mem
         FROM {{ ref('colorado_all_payers_member_eligibility_stage') }}
)
, claim_header_fields AS (
       SELECT a.*
            , b.*
            , REPLACE(
                     ROUND(
                            DIV0(a.total_amt * 1.0, b.cnt_all_mem)::FLOAT
                          , 2
                     )
                   , '.'
                   , ''
              ) AS HD007
            , REPLACE(
                     ROUND(
                            DIV0(
                                   a.total_amt_dental * 1.0
                                 , b.cnt_mem_dental_eligible
                            )::FLOAT
                          , 2
                     )
                   , '.'
                   , ''
              ) AS HD009
            , REPLACE(
                     ROUND(
                            DIV0(
                                   a.total_amt_vision * 1.0
                                 , b.cnt_mem_vision_eligible
                            )::FLOAT
                          , 2
                     )
                   , '.'
                   , ''
              ) AS HD010
         FROM claims_count AS a
            , mem_eligible AS b
)
, header_stage AS (
       SELECT CONCAT_WS(
                     '|'
                   , 'HD' -- HD001 HEADER INDICATOR
                   , 'MC' -- HD002 RECORD TYPE
                   , 'COC0135' -- HD003 PAYER CODE
                   , 'DHP_COC0135' -- HD004 PAYER NAME
                   , report_month -- HD005 BEGINNING MONTH
                   , report_month -- HD006 ENDING MONTH
                   , IFNULL(claim_count, 0) -- HD007 RECORD COUNT
                   , HD007 -- HD008 MED_BH PMPM
                   , '' -- HD009 PHARMACY PMPM (leave blank)
                   , HD009 -- HD010 DENTAL PMPM
                   , HD010 -- HD011 VISION PMPM
                   , CASE WHEN '{{ var("file_env") }}' = 'TEST' THEN 'T' ELSE 'P' END -- HD012 FILE TYPE INDICATOR (P or T)
              ) AS text_blob
            , 1 AS chunk_order
         FROM claim_header_fields
)
, base_stage AS (
       {% set all_columns = adapter.get_columns_in_relation(ref('colorado_all_payers_claim_stage')) %}
       {% set except_col_names=["FINALIZED_DATE_EASTERN", "ADJUDICATION_ID", "LINE_ADJUDICATION_ID","PLAN_TYPE", "FIRST_SERVICE_DATE", "TARGET_MONTH", "CLAIM_STAGE_ID","BILLING_PROVIDER_RECORD_LOCATOR","RENDERING_PROVIDER_RECORD_LOCATOR"] %}
       {% set col_names_to_hardcode=["MC999999"] %}
       -- create data rows with pipe-delimited values
       SELECT
              concat_ws('|',
              {%- for col in all_columns if col.name not in except_col_names %}
                     IFNULL(
                            {%- if  col.name in col_names_to_hardcode %}
                                   '20000219', '')
                            {%- else %}
                                   REPLACE(REPLACE({{ col.name }},',',''),'\n',''), '')
                            {% endif %}
                     {%- if not loop.last %} {{ ',' }}
                     {% endif %}
              {%- endfor %}) as text_blob
              , 2 as chunk_order
       FROM claims
)
, trailer_stage AS (
       SELECT CONCAT_WS(
                     '|'
                   , 'TR' -- TR001 TRAILER INDICATOR
                   , 'MC' -- TR002 RECORD TYPE
                   , 'COC0135' -- TR003 PAYER CODE
                   , 'DHP_COC0135' -- TR004 PAYER NAME
                   , report_month -- TR005 BEGINNING MONTH
                   , report_month -- TR006 ENDING MONTH
                   , to_char(current_timestamp, 'yyyymmdd') -- TR007 DATE CREATED
              ) AS text_blob
            , 3 AS chunk_order
         FROM claims_count
)
, aggregated AS (
       SELECT *, TRY_TO_TIMESTAMP('{{ var("data_anchor_month") }}') AS target_month
         FROM header_stage
        UNION ALL
       SELECT *, TRY_TO_TIMESTAMP('{{ var("data_anchor_month") }}') AS target_month
         FROM base_stage
        UNION ALL
       SELECT *, TRY_TO_TIMESTAMP('{{ var("data_anchor_month") }}') AS target_month
         FROM trailer_stage
)
SELECT
       text_blob, chunk_order, target_month,
       {{ dbt_utils.generate_surrogate_key(
              [
                     'target_month',
                     'text_blob'
              ]
       )
       }} AS claim_medical_id
  FROM aggregated
  ORDER BY chunk_order
)))))__SQLFMT_OUTPUT__(((((
-- disable-parser: if statement creates invalid sql
{{ config(
    materialized="table",
    meta={'final_schema': 'integrations'}
) }}
/*         COLORADO CLAIMS MEDICAL EXTRACT
           dbtmodel: colorado_all_payers_claim_medical.sql
           Purpose:
           - Pull all data from transform.colorado_all_payers_claim_stage and format them as per the Colorado APCD requirements
           - Remove the fields that are for internal purposes
           - DETAILS of REPORT in this link:
             -- https://civhc.org/wp-content/uploads/2024/12/Data-Submission-Guide-DSG-v-16-Final.pdf
*/
WITH claims AS (
       SELECT claims.*
         FROM {{ ref('colorado_all_payers_claim_stage') }} AS claims
 WHERE target_month = TRY_TO_TIMESTAMP('{{ var("data_anchor_month") }}')
)
, claims_header_amts AS (
       SELECT *
            , MC063::FLOAT + MC064::FLOAT + MC065::FLOAT + MC066::FLOAT + MC067::FLOAT AS amt_per_record
            , IFF(MC220 = 'Y', amt_per_record, 0.0) AS amt_vision
            , IFF(MC209 = 'Y', amt_per_record, 0.0) AS amt_dental
            , IFF(MC209 != 'Y' AND MC220 != 'Y', amt_per_record, 0.0) AS amt_other
         FROM claims
)
, claims_count AS (
       SELECT COUNT(*) AS claim_count
            , TO_CHAR(
                     TRY_TO_TIMESTAMP('{{ var("data_anchor_month") }}')
                   , 'YYYYMM'
              ) AS report_month
            , SUM(amt_vision) AS total_amt_vision
            , SUM(amt_dental) AS total_amt_dental
            , SUM(amt_other) AS total_other_amt
            , SUM(amt_per_record) AS total_amt
         FROM claims_header_amts
)
, mem_eligible AS (
       SELECT COUNT( DISTINCT
                       CASE WHEN ME152 = 'Y' THEN member_id ELSE NULL END
              ) AS cnt_mem_vision_eligible
            , COUNT( DISTINCT
                       CASE WHEN ME020 = 'Y' THEN member_id ELSE NULL END
              ) AS cnt_mem_dental_eligible
            , COUNT( DISTINCT
                       CASE
                       WHEN ME018 = 'Y' OR ME123 = 'Y' THEN member_id
                       ELSE NULL
                        END
              ) AS cnt_all_mem
         FROM {{ ref('colorado_all_payers_member_eligibility_stage') }}
)
, claim_header_fields AS (
       SELECT a.*
            , b.*
            , REPLACE(
                     ROUND(
                            DIV0(a.total_amt * 1.0, b.cnt_all_mem)::FLOAT
                          , 2
                     )
                   , '.'
                   , ''
              ) AS HD007
            , REPLACE(
                     ROUND(
                            DIV0(
                                   a.total_amt_dental * 1.0
                                 , b.cnt_mem_dental_eligible
                            )::FLOAT
                          , 2
                     )
                   , '.'
                   , ''
              ) AS HD009
            , REPLACE(
                     ROUND(
                            DIV0(
                                   a.total_amt_vision * 1.0
                                 , b.cnt_mem_vision_eligible
                            )::FLOAT
                          , 2
                     )
                   , '.'
                   , ''
              ) AS HD010
         FROM claims_count AS a
            , mem_eligible AS b
)
, header_stage AS (
       SELECT CONCAT_WS(
                     '|'
                   , 'HD' -- HD001 HEADER INDICATOR
                   , 'MC' -- HD002 RECORD TYPE
                   , 'COC0135' -- HD003 PAYER CODE
                   , 'DHP_COC0135' -- HD004 PAYER NAME
                   , report_month -- HD005 BEGINNING MONTH
                   , report_month -- HD006 ENDING MONTH
                   , IFNULL(claim_count, 0) -- HD007 RECORD COUNT
                   , HD007 -- HD008 MED_BH PMPM
                   , '' -- HD009 PHARMACY PMPM (leave blank)
                   , HD009 -- HD010 DENTAL PMPM
                   , HD010 -- HD011 VISION PMPM
                   , CASE WHEN '{{ var("file_env") }}' = 'TEST' THEN 'T' ELSE 'P' END -- HD012 FILE TYPE INDICATOR (P or T)
              ) AS text_blob
            , 1 AS chunk_order
         FROM claim_header_fields
)
, base_stage AS (
       {% set all_columns = adapter.get_columns_in_relation(ref('colorado_all_payers_claim_stage')) %}
       {% set except_col_names=["FINALIZED_DATE_EASTERN", "ADJUDICATION_ID", "LINE_ADJUDICATION_ID","PLAN_TYPE", "FIRST_SERVICE_DATE", "TARGET_MONTH", "CLAIM_STAGE_ID","BILLING_PROVIDER_RECORD_LOCATOR","RENDERING_PROVIDER_RECORD_LOCATOR"] %}
       {% set col_names_to_hardcode=["MC999999"] %}
       -- create data rows with pipe-delimited values
       SELECT
              concat_ws('|',
              {%- for col in all_columns if col.name not in except_col_names %}
                     IFNULL(
                            {%- if  col.name in col_names_to_hardcode %}
                                   '20000219', '')
                            {%- else %}
                                   REPLACE(REPLACE({{ col.name }},',',''),'\n',''), '')
                            {% endif %}
                     {%- if not loop.last %} {{ ',' }}
                     {% endif %}
              {%- endfor %}) as text_blob
              , 2 as chunk_order
       FROM claims
)
, trailer_stage AS (
       SELECT CONCAT_WS(
                     '|'
                   , 'TR' -- TR001 TRAILER INDICATOR
                   , 'MC' -- TR002 RECORD TYPE
                   , 'COC0135' -- TR003 PAYER CODE
                   , 'DHP_COC0135' -- TR004 PAYER NAME
                   , report_month -- TR005 BEGINNING MONTH
                   , report_month -- TR006 ENDING MONTH
                   , to_char(current_timestamp, 'yyyymmdd') -- TR007 DATE CREATED
              ) AS text_blob
            , 3 AS chunk_order
         FROM claims_count
)
, aggregated AS (
       SELECT *, TRY_TO_TIMESTAMP('{{ var("data_anchor_month") }}') AS target_month
         FROM header_stage
        UNION ALL
       SELECT *, TRY_TO_TIMESTAMP('{{ var("data_anchor_month") }}') AS target_month
         FROM base_stage
        UNION ALL
       SELECT *, TRY_TO_TIMESTAMP('{{ var("data_anchor_month") }}') AS target_month
         FROM trailer_stage
)
SELECT
       text_blob, chunk_order, target_month,
       {{ dbt_utils.generate_surrogate_key(
              [
                     'target_month',
                     'text_blob'
              ]
       )
       }} AS claim_medical_id
  FROM aggregated
  ORDER BY chunk_order
