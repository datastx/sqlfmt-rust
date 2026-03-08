{% test unique_dynamic_warehouse(model, column_name, snowflake_warehouse) %}
    {% if snowflake_warehouse is none %}
        {{
            exceptions.raise_compiler_error(
                "❌ 'snowflake_warehouse' parameter must not be null in production. Please provide a valid warehouse name."
            )
        }}
    {% endif %}
    {% if target.name == "prod" %}
        {{ config(snowflake_warehouse=snowflake_warehouse) }}
    {% endif %}
    with
        duplicates as (
            select
                {{ column_name }} as duplicate_value
                , count(*) as duplicate_count
            from {{ model }}
            group by {{ column_name }}
            having count(*) > 1
        )
    select
        duplicate_value
        , duplicate_count
    from duplicates
{% endtest %}
