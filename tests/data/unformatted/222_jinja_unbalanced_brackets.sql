-- Jinja if/else with unbalanced brackets per branch (each branch
-- independently closes the IFNULL paren) plus Snowflake :: casts.
{{ config(materialized="table") }}
WITH base AS (
    SELECT a::FLOAT + b::FLOAT AS total
    FROM {{ ref('source_table') }}
    WHERE created_at > '{{ var("start_date") }}'
)
, dynamic_cols AS (
    {% set columns = adapter.get_columns_in_relation(ref('source_table')) %}
    {% set skip_cols = ["INTERNAL_ID", "UPDATED_AT"] %}
    {% set special_cols = ["LEGACY_CODE"] %}
    SELECT
        concat_ws('|',
        {%- for col in columns if col.name not in skip_cols %}
            IFNULL(
                {%- if col.name in special_cols %}
                    '19700101', '')
                {%- else %}
                    REPLACE({{ col.name }}, ',', ''), '')
                {% endif %}
                {%- if not loop.last %} {{ ',' }}
                {% endif %}
        {%- endfor %}) AS row_data
    FROM base
)
SELECT row_data
FROM dynamic_cols
)))))__SQLFMT_OUTPUT__(((((
-- Jinja if/else with unbalanced brackets per branch (each branch
-- independently closes the IFNULL paren) plus Snowflake :: casts.
{{ config(materialized="table") }}
with
    base as (
        select a::float + b::float as total
        from {{ ref("source_table") }}
        where created_at > '{{ var("start_date") }}'
    )
    , dynamic_cols as (
        {% set columns = adapter.get_columns_in_relation(ref("source_table")) %}
        {% set skip_cols = ["INTERNAL_ID", "UPDATED_AT"] %}
        {% set special_cols = ["LEGACY_CODE"] %}
        select
            concat_ws(
                '|'
                ,
                {%- for col in columns if col.name not in skip_cols %}
                    ifnull(
                    {%- if col.name in special_cols %}
                            '19700101'
                            , ''
                        )
                        {%- else %}
                            replace(
                                {{ col.name }}
                                , ','
                                , ''
                            )
                            , ''
                        )
                    {% endif %}
                        {%- if not loop.last %} {{ "," }} {% endif %}
                {%- endfor %}
            ) as row_data
        from base
    )
select row_data
from dynamic_cols
