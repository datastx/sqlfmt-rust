{% macro dbt_snowflake_validate_get_incremental_full_refresh_on_schema_change_strategy(config) %}
  {#-- Find and validate the incremental strategy #}
  {%- set strategy = config.get("incremental_strategy", default="merge") -%}

  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    Expected one of: 'merge', 'delete+insert', 'merge+check'
  {%- endset %}
  {% if strategy not in ['merge', 'delete+insert', 'merge+check'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}

  {% do return(strategy) %}
{% endmacro %}

{% macro dbt_snowflake_get_incremental_full_refresh_on_schema_change_sql(strategy, tmp_relation, target_relation, unique_key, dest_columns, check_cols, exclude_from_check_cols) %}
  {% if strategy == 'merge' %}
    {% do return(get_merge_sql(target_relation, tmp_relation, unique_key, dest_columns)) %}
  {% elif strategy == 'merge+check' %}
    {% do return(get_merge_check_sql(target_relation, tmp_relation, unique_key, dest_columns, check_cols, exclude_from_check_cols)) %}
  {% elif strategy == 'delete+insert' %}
    {% do return(get_delete_insert_merge_sql(target_relation, tmp_relation, unique_key, dest_columns)) %}
  {% else %}
    {% do exceptions.raise_compiler_error('invalid strategy: ' ~ strategy) %}
  {% endif %}
{% endmacro %}

{% macro get_merge_check_sql(target, source, unique_key, dest_columns, check_cols_config, exclude_from_check_cols) -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set update_columns = config.get('merge_update_columns', default = dest_columns | map(attribute="quoted") | list) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {{ sql_header if sql_header is not none }}

    merge into {{ target }} as DBT_INTERNAL_DEST
        using {{ source }} as DBT_INTERNAL_SOURCE
        on DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}

    {% if check_cols_config == 'all' %}
      {% set column_added, unfiltered_check_cols = snapshot_check_all_get_existing_columns(model, True) %}
    {% elif check_cols_config is iterable and (check_cols_config | length) > 0 %}
      {% set unfiltered_check_cols = check_cols_config %}
    {% else %}
      {% do exceptions.raise_compiler_error("Invalid value for 'check_cols': " ~ check_cols_config) %}
    {% endif %}

    {% if (exclude_from_check_cols | length) > 0 %}
        {% do exclude_from_check_cols.append(unique_key) %}
    {% else %}
      {% set exclude_from_check_cols = [unique_key] %}
    {% endif %}
    {% set check_cols = compare_lists(uppercase_list(unfiltered_check_cols), uppercase_list(exclude_from_check_cols)) %}

    when matched and
      {% for col in check_cols -%}
          DBT_INTERNAL_DEST.{{ col }} != DBT_INTERNAL_SOURCE.{{ col }}
          or
          (
              ((DBT_INTERNAL_DEST.{{ col }} is null) and not (DBT_INTERNAL_SOURCE.{{ col }} is null))
              or
              ((not DBT_INTERNAL_DEST.{{ col }} is null) and (DBT_INTERNAL_SOURCE.{{ col }} is null))
          )
          {%- if not loop.last %} or {% endif -%}
      {%- endfor %} then update set
      {% for column_name in update_columns -%}
          {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
          {%- if not loop.last %}, {%- endif %}
      {%- endfor %}

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

{% endmacro %}



-- based on incremental materialzation
-- adds changed_cols option that automatically triggers full refresh if columns are changed
{% materialization incremental_full_refresh_on_schema_change, adapter='snowflake' -%}

  {% set original_query_tag = set_query_tag() %}

  {%- set unique_key = config.get('unique_key') -%}
  {%- set check_cols = config.get('check_cols') -%}
  {%- set exclude_from_check_cols = config.get('exclude_from_check_cols') -%}

  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {% set target_relation = this %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {#-- Validate early so we don't run SQL if the strategy is invalid --#}
  {% set strategy = dbt_snowflake_validate_get_incremental_full_refresh_on_schema_change_strategy(config) -%}

  -- setup
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if existing_relation is none %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view %}
    {#-- Can't overwrite a view with a table - we must drop --#}
    {{ log("Dropping relation " ~ target_relation ~ " because it is a view and this model is a table.", info=true) }}
    {% do adapter.drop_relation(existing_relation) %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% elif full_refresh_mode %}
    {% set build_sql = create_table_as(False, target_relation, sql) %}
  {% else %}
    {% do run_query(create_table_as(True, tmp_relation, sql)) %}
    {% do adapter.expand_target_column_types(
           from_relation=tmp_relation,
           to_relation=target_relation) %}
    {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
    {% set build_sql = dbt_snowflake_get_incremental_full_refresh_on_schema_change_sql(strategy, tmp_relation, target_relation, unique_key, dest_columns, check_cols, exclude_from_check_cols) %}
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {% set target_relation = target_relation.incorporate(type='table') %}
  {% do persist_docs(target_relation, model) %}

  {% do unset_query_tag(original_query_tag) %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
)))))__SQLFMT_OUTPUT__(((((
{% macro dbt_snowflake_validate_get_incremental_full_refresh_on_schema_change_strategy(
    config
) %}
    {#-- Find and validate the incremental strategy #}
    {%- set strategy = config.get("incremental_strategy", default="merge") -%}

    {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    Expected one of: 'merge', 'delete+insert', 'merge+check'
    {%- endset %}
    {% if strategy not in ["merge", "delete+insert", "merge+check"] %}
        {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
    {% endif %}

    {% do return(strategy) %}
{% endmacro %}

{% macro dbt_snowflake_get_incremental_full_refresh_on_schema_change_sql(
    strategy,
    tmp_relation,
    target_relation,
    unique_key,
    dest_columns,
    check_cols,
    exclude_from_check_cols
) %}
    {% if strategy == "merge" %}
        {% do return(
            get_merge_sql(target_relation, tmp_relation, unique_key, dest_columns)
        ) %}
    {% elif strategy == "merge+check" %}
        {% do return(
            get_merge_check_sql(target_relation, tmp_relation, unique_key, dest_columns, check_cols, exclude_from_check_cols)
        ) %}
    {% elif strategy == "delete+insert" %}
        {% do return(
            get_delete_insert_merge_sql(target_relation, tmp_relation, unique_key, dest_columns)
        ) %}
    {% else %} {% do exceptions.raise_compiler_error("invalid strategy: " ~ strategy) %}
    {% endif %}
{% endmacro %}

{% macro get_merge_check_sql(
    target,
    source,
    unique_key,
    dest_columns,
    check_cols_config,
    exclude_from_check_cols
) -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set update_columns = config.get(
        "merge_update_columns",
        default = dest_columns | map(attribute="quoted") | list
    ) -%}
    {%- set sql_header = config.get("sql_header", none) -%}

    {{ sql_header if sql_header is not none }}

    merge into {{ target }} as DBT_INTERNAL_DEST
        using {{ source }} as DBT_INTERNAL_SOURCE
        on DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}

    {% if check_cols_config == "all" %}
        {% set column_added, unfiltered_check_cols = snapshot_check_all_get_existing_columns(
            model,
            True
        ) %}
    {% elif check_cols_config is iterable and (check_cols_config | length) > 0 %}
        {% set unfiltered_check_cols = check_cols_config %}
    {% else %}
        {% do exceptions.raise_compiler_error(
            "Invalid value for 'check_cols': " ~ check_cols_config
        ) %}
    {% endif %}

    {% if (exclude_from_check_cols | length) > 0 %}
        {% do exclude_from_check_cols.append(unique_key) %}
    {% else %} {% set exclude_from_check_cols = [unique_key] %}
    {% endif %}
    {% set check_cols = compare_lists(
        uppercase_list(unfiltered_check_cols),
        uppercase_list(exclude_from_check_cols)
    ) %}

    when matched and
    {% for col in check_cols -%}
          DBT_INTERNAL_DEST.{{ col }} != DBT_INTERNAL_SOURCE.{{ col }}
          or
          (
              ((DBT_INTERNAL_DEST.{{ col }} is null) and not (DBT_INTERNAL_SOURCE.{{ col }} is null))
              or
              ((not DBT_INTERNAL_DEST.{{ col }} is null) and (DBT_INTERNAL_SOURCE.{{ col }} is null))
          )
          {%- if not loop.last %} or {% endif -%}
      {%- endfor %} then update set
        {% for column_name in update_columns -%}
          {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
          {%- if not loop.last %}, {%- endif %}
            {%- endfor %}

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

        {% endmacro %}

        -- based on incremental materialzation
        -- adds changed_cols option that automatically triggers full refresh if columns
        -- are changed
        {% 
            materialization incremental_full_refresh_on_schema_change, adapter = "snowflake"
        -%}

            {% set original_query_tag = set_query_tag() %}

            {%- set unique_key = config.get("unique_key") -%}
            {%- set check_cols = config.get("check_cols") -%}
            {%- set exclude_from_check_cols = config.get("exclude_from_check_cols") -%}

            {%- set full_refresh_mode = (should_full_refresh()) -%}

            {% set target_relation = this %}
            {% set existing_relation = load_relation(this) %}
            {% set tmp_relation = make_temp_relation(this) %}

            {#-- Validate early so we don't run SQL if the strategy is invalid --#}
            {% set strategy = dbt_snowflake_validate_get_incremental_full_refresh_on_schema_change_strategy(
                config
            ) -%}

            -- setup
            {{ run_hooks(pre_hooks, inside_transaction=False) }}

            -- `BEGIN` happens here:
            {{ run_hooks(pre_hooks, inside_transaction=True) }}

            {% if existing_relation is none %}
                {% set build_sql = create_table_as(False, target_relation, sql) %}
            {% elif existing_relation.is_view %}
                {#-- Can't overwrite a view with a table - we must drop --#}
                {{
                    log(
                        "Dropping relation " ~ target_relation ~ " because it is a view and this model is a table.",
                        info=true,
                    )
                }}
                {% do adapter.drop_relation(existing_relation) %}
                {% set build_sql = create_table_as(False, target_relation, sql) %}
            {% elif full_refresh_mode %}
                {% set build_sql = create_table_as(False, target_relation, sql) %}
            {% else %} {% do run_query(create_table_as(True, tmp_relation, sql)) %}
                {% do adapter.expand_target_column_types(
                    from_relation=tmp_relation,
                    to_relation=target_relation
                ) %}
                {% set dest_columns = adapter.get_columns_in_relation(
                    target_relation
                ) %}
                {% set build_sql = dbt_snowflake_get_incremental_full_refresh_on_schema_change_sql(
                    strategy,
                    tmp_relation,
                    target_relation,
                    unique_key,
                    dest_columns,
                    check_cols,
                    exclude_from_check_cols
                ) %}
            {% endif %}

            {%- call statement("main") -%}
            {{ build_sql }}
        {%- endcall -%}

        {{ run_hooks(post_hooks, inside_transaction=True) }}

        -- `COMMIT` happens here
        {{ adapter.commit() }}

        {{ run_hooks(post_hooks, inside_transaction=False) }}

        {% set target_relation = target_relation.incorporate(type="table") %}
        {% do persist_docs(target_relation, model) %}

        {% do unset_query_tag(original_query_tag) %}

        {{ return({"relations": [target_relation]}) }}

    {%- endmaterialization %}
