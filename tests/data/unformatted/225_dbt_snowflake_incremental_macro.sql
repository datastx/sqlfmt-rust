-- Regression test: {% if (var | filter) > 0 %} must preserve space between
-- keyword and paren so the lexer identifies it as JinjaBlockStart, not
-- JinjaStatement.
{% macro validate_strategy(config) %}
  {#-- Validate the strategy #}
  {%- set strategy = config.get("strategy", default="merge") -%}

  {% set error_msg -%}
    Invalid strategy: {{ strategy }}
    Expected one of: 'merge', 'append', 'upsert'
  {%- endset %}
  {% if strategy not in ['merge', 'append', 'upsert'] %}
    {% do exceptions.raise_compiler_error(error_msg) %}
  {% endif %}

  {% do return(strategy) %}
{% endmacro %}

{% macro get_incremental_sql(strategy, source_rel, target_rel, pk, columns, check_list, exclude_list) %}
  {% if strategy == 'merge' %}
    {% do return(build_merge_sql(target_rel, source_rel, pk, columns)) %}
  {% elif strategy == 'upsert' %}
    {% do return(build_upsert_sql(target_rel, source_rel, pk, columns, check_list, exclude_list)) %}
  {% elif strategy == 'append' %}
    {% do return(build_append_sql(target_rel, source_rel, pk, columns)) %}
  {% else %}
    {% do exceptions.raise_compiler_error('unknown strategy: ' ~ strategy) %}
  {% endif %}
{% endmacro %}

{% macro build_upsert_sql(tgt, src, pk, columns, check_cfg, exclude_cfg) -%}
    {%- set cols_csv = get_quoted_csv(columns | map(attribute="name")) -%}
    {%- set upd_columns = config.get('update_columns', default = columns | map(attribute="quoted") | list) -%}
    {%- set header = config.get('sql_header', none) -%}

    {{ header if header is not none }}

    merge into {{ tgt }} as TGT
        using {{ src }} as SRC
        on SRC.{{ pk }} = TGT.{{ pk }}

    {% if check_cfg == 'all' %}
      {% set found, raw_check_cols = get_all_existing_columns(model, True) %}
    {% elif check_cfg is iterable and (check_cfg | length) > 0 %}
      {% set raw_check_cols = check_cfg %}
    {% else %}
      {% do exceptions.raise_compiler_error("Bad value for 'check_cfg': " ~ check_cfg) %}
    {% endif %}

    {% if (exclude_cfg | length) > 0 %}
        {% do exclude_cfg.append(pk) %}
    {% else %}
      {% set exclude_cfg = [pk] %}
    {% endif %}
    {% set check_cols = diff_lists(upper_list(raw_check_cols), upper_list(exclude_cfg)) %}

    when matched and
      {% for col in check_cols -%}
          TGT.{{ col }} != SRC.{{ col }}
          or
          (
              ((TGT.{{ col }} is null) and not (SRC.{{ col }} is null))
              or
              ((not TGT.{{ col }} is null) and (SRC.{{ col }} is null))
          )
          {%- if not loop.last %} or {% endif -%}
      {%- endfor %} then update set
      {% for c in upd_columns -%}
          {{ c }} = SRC.{{ c }}
          {%- if not loop.last %}, {%- endif %}
      {%- endfor %}

    when not matched then insert
        ({{ cols_csv }})
    values
        ({{ cols_csv }})

{% endmacro %}


{% materialization my_incremental, adapter='snowflake' -%}

  {% set qtag = set_query_tag() %}

  {%- set pk = config.get('unique_key') -%}
  {%- set check_list = config.get('check_cols') -%}
  {%- set exclude_list = config.get('exclude_cols') -%}

  {%- set do_full_refresh = (should_full_refresh()) -%}

  {% set target_rel = this %}
  {% set existing_rel = load_relation(this) %}
  {% set tmp_rel = make_temp_relation(this) %}

  {#-- Validate early so we don't run SQL if the strategy is invalid --#}
  {% set strategy = validate_strategy(config) -%}

  -- setup
  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- begin
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if existing_rel is none %}
    {% set build_sql = create_table_as(False, target_rel, sql) %}
  {% elif existing_rel.is_view %}
    {#-- Can't overwrite a view with a table - we must drop --#}
    {{ log("Dropping " ~ target_rel ~ " because it is a view and this model is a table.", info=true) }}
    {% do adapter.drop_relation(existing_rel) %}
    {% set build_sql = create_table_as(False, target_rel, sql) %}
  {% elif do_full_refresh %}
    {% set build_sql = create_table_as(False, target_rel, sql) %}
  {% else %}
    {% do run_query(create_table_as(True, tmp_rel, sql)) %}
    {% do adapter.expand_target_column_types(
           from_relation=tmp_rel,
           to_relation=target_rel) %}
    {% set columns = adapter.get_columns_in_relation(target_rel) %}
    {% set build_sql = get_incremental_sql(strategy, tmp_rel, target_rel, pk, columns, check_list, exclude_list) %}
  {% endif %}

  {%- call statement('main') -%}
    {{ build_sql }}
  {%- endcall -%}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- commit
  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {% set target_rel = target_rel.incorporate(type='table') %}
  {% do persist_docs(target_rel, model) %}

  {% do unset_query_tag(qtag) %}

  {{ return({'relations': [target_rel]}) }}

{%- endmaterialization %}
)))))__SQLFMT_OUTPUT__(((((
-- Regression test: {% if (var | filter) > 0 %} must preserve space between
-- keyword and paren so the lexer identifies it as JinjaBlockStart, not
-- JinjaStatement.
{% macro validate_strategy(config) %}
    {#-- Validate the strategy #}
    {%- set strategy = config.get("strategy", default="merge") -%} {% set error_msg -%}
    Invalid strategy: {{ strategy }}
    Expected one of: 'merge', 'append', 'upsert'
    {%- endset %}
    {% if strategy not in ["merge", "append", "upsert"] %}
        {% do exceptions.raise_compiler_error(error_msg) %}
    {% endif %}

    {% do return(strategy) %}
{% endmacro %}

{% macro get_incremental_sql(
    strategy,
    source_rel,
    target_rel,
    pk,
    columns,
    check_list,
    exclude_list
) %}
    {% if strategy == "merge" %}
        {% do return(build_merge_sql(target_rel, source_rel, pk, columns)) %}
    {% elif strategy == "upsert" %}
        {% do return(
            build_upsert_sql(target_rel, source_rel, pk, columns, check_list, exclude_list)
        ) %}
    {% elif strategy == "append" %}
        {% do return(build_append_sql(target_rel, source_rel, pk, columns)) %}
    {% else %} {% do exceptions.raise_compiler_error("unknown strategy: " ~ strategy) %}
    {% endif %}
{% endmacro %}

{% macro build_upsert_sql(tgt, src, pk, columns, check_cfg, exclude_cfg) -%}
    {%- set cols_csv = get_quoted_csv(columns | map(attribute="name")) -%}
    {%- set upd_columns = config.get(
        "update_columns",
        default = columns | map(attribute="quoted") | list
    ) -%}
    {%- set header = config.get("sql_header", none) -%}

    {{ header if header is not none }}

    merge into {{ tgt }} as TGT
        using {{ src }} as SRC
        on SRC.{{ pk }} = TGT.{{ pk }}

    {% if check_cfg == "all" %}
        {% set found, raw_check_cols = get_all_existing_columns(model, True) %}
    {% elif check_cfg is iterable and (check_cfg | length) > 0 %}
        {% set raw_check_cols = check_cfg %}
    {% else %}
        {% do exceptions.raise_compiler_error(
            "Bad value for 'check_cfg': " ~ check_cfg
        ) %}
    {% endif %}

    {% if (exclude_cfg | length) > 0 %} {% do exclude_cfg.append(pk) %}
    {% else %} {% set exclude_cfg = [pk] %}
    {% endif %}
    {% set check_cols = diff_lists(
        upper_list(raw_check_cols),
        upper_list(exclude_cfg)
    ) %}

    when matched and
    {% for col in check_cols -%}
          TGT.{{ col }} != SRC.{{ col }}
          or
          (
              ((TGT.{{ col }} is null) and not (SRC.{{ col }} is null))
              or
              ((not TGT.{{ col }} is null) and (SRC.{{ col }} is null))
          )
          {%- if not loop.last %} or {% endif -%}
      {%- endfor %} then update set
        {% for c in upd_columns -%}
          {{ c }} = SRC.{{ c }}
          {%- if not loop.last %}, {%- endif %}
            {%- endfor %}

    when not matched then insert
        ({{ cols_csv }})
    values
        ({{ cols_csv }})

        {% endmacro %}

        {% materialization my_incremental, adapter = "snowflake" -%}

            {% set qtag = set_query_tag() %}

            {%- set pk = config.get("unique_key") -%}
            {%- set check_list = config.get("check_cols") -%}
            {%- set exclude_list = config.get("exclude_cols") -%}

            {%- set do_full_refresh = (should_full_refresh()) -%}

            {% set target_rel = this %}
            {% set existing_rel = load_relation(this) %}
            {% set tmp_rel = make_temp_relation(this) %}

            {#-- Validate early so we don't run SQL if the strategy is invalid --#}
            {% set strategy = validate_strategy(config) -%}

            -- setup
            {{ run_hooks(pre_hooks, inside_transaction=False) }}

            -- begin
            {{ run_hooks(pre_hooks, inside_transaction=True) }}

            {% if existing_rel is none %}
                {% set build_sql = create_table_as(False, target_rel, sql) %}
            {% elif existing_rel.is_view %}
                {#-- Can't overwrite a view with a table - we must drop --#}
                {{
                    log(
                        "Dropping " ~ target_rel ~ " because it is a view and this model is a table.",
                        info=true,
                    )
                }}
                {% do adapter.drop_relation(existing_rel) %}
                {% set build_sql = create_table_as(False, target_rel, sql) %}
            {% elif do_full_refresh %}
                {% set build_sql = create_table_as(False, target_rel, sql) %}
            {% else %} {% do run_query(create_table_as(True, tmp_rel, sql)) %}
                {% do adapter.expand_target_column_types(
                    from_relation=tmp_rel,
                    to_relation=target_rel
                ) %}
                {% set columns = adapter.get_columns_in_relation(target_rel) %}
                {% set build_sql = get_incremental_sql(
                    strategy,
                    tmp_rel,
                    target_rel,
                    pk,
                    columns,
                    check_list,
                    exclude_list
                ) %}
            {% endif %}

            {%- call statement("main") -%}
            {{ build_sql }}
        {%- endcall -%}

        {{ run_hooks(post_hooks, inside_transaction=True) }}

        -- commit
        {{ adapter.commit() }}

        {{ run_hooks(post_hooks, inside_transaction=False) }}

        {% set target_rel = target_rel.incorporate(type="table") %}
        {% do persist_docs(target_rel, model) %}

        {% do unset_query_tag(qtag) %}

        {{ return({"relations": [target_rel]}) }}

    {%- endmaterialization %}
