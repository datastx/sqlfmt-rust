{% macro get_column_metadata(node_id, attr_key) %}
	{% if execute %}
		{% set results = [] %}
		{% set cols = graph.nodes[node_id]['columns']  %}
		{% if attr_key is not none %}
			{% for col in cols if graph.nodes[node_id]['columns'][col]['meta'][attr_key] | length > 0 %}
				{% set meta_dict = graph.nodes[node_id]['columns'][col]['meta'] %}
				{% if attr_key in meta_dict %}
					{% set attr_val = meta_dict[attr_key] %}
					{% if "extra_inputs" in meta_dict %}
						{% set extra = meta_dict['extra_inputs'] %}
					{% else %}
						{% set extra = [] %}
					{% endif %}
					{% set row = (col, attr_val, extra) %}
					{% do results.append(row) %}
				{% endif %}
			{% endfor %}
			{% for col in cols if graph.nodes[node_id]['columns'][col].get('config', {}).get('meta', {}).get(attr_key, '') | length > 0 %}
				{% set meta_dict = graph.nodes[node_id]['columns'][col]['config']['meta'] %}
				{% if attr_key in meta_dict %}
					{% set attr_val = meta_dict[attr_key] %}
					{% if "extra_inputs" in meta_dict %}
						{% set extra = meta_dict['extra_inputs'] %}
					{% else %}
						{% set extra = [] %}
					{% endif %}
					{% set row = (col, attr_val, extra) %}
					{% do results.append(row) %}
				{% endif %}
			{% endfor %}
		{% else %}
			{% do results.append(col|upper) %}
		{% endif %}
		{{ return(results) }}
	{% endif %}
{% endmacro %}

)))))__SQLFMT_OUTPUT__(((((
{% macro get_column_metadata(node_id, attr_key) %}
    {% if execute %}
        {% set results = [] %}
        {% set cols = graph.nodes[node_id]["columns"] %}
        {% if attr_key is not none %}
            {% 
                for col in cols if graph.nodes[node_id]["columns"][col]["meta"][attr_key] | length > 0
            %}
                {% set meta_dict = graph.nodes[node_id]["columns"][col]["meta"] %}
                {% if attr_key in meta_dict %}
                    {% set attr_val = meta_dict[attr_key] %}
                    {% if "extra_inputs" in meta_dict %}
                        {% set extra = meta_dict["extra_inputs"] %}
                    {% else %} {% set extra = [] %}
                    {% endif %}
                    {% set row = (col, attr_val, extra) %}
                    {% do results.append(row) %}
                {% endif %}
            {% endfor %}
            {% for col in cols if graph.nodes[node_id]["columns"][col].get(
                "config",
                {}
            ).get("meta", {}).get(attr_key, "") | length > 0 %}
                {% set meta_dict = graph.nodes[node_id]["columns"][col]["config"]["meta"] %}
                {% if attr_key in meta_dict %}
                    {% set attr_val = meta_dict[attr_key] %}
                    {% if "extra_inputs" in meta_dict %}
                        {% set extra = meta_dict["extra_inputs"] %}
                    {% else %} {% set extra = [] %}
                    {% endif %}
                    {% set row = (col, attr_val, extra) %}
                    {% do results.append(row) %}
                {% endif %}
            {% endfor %}
        {% else %} {% do results.append(col | upper) %}
        {% endif %}
        {{ return(results) }}
    {% endif %}
{% endmacro %}
