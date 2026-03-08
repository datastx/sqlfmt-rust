{% macro test_fn() %}
    {% if condition %}
        {% for item in collection.values() %}
            {% set item_group_path = item.path.split("/")[:publish_model_group_path_len] %}
        {% endfor %}
    {% endif %}
{% endmacro %}
