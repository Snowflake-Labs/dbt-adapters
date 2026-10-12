{% macro snowflake__scalar_function_sql(target_relation) %}
    {% if snowflake__is_data_metric_function() %}
        {{ snowflake__data_metric_function_sql(target_relation) }}
    {% else %}
        {{ default__scalar_function_sql(target_relation) }}
    {% endif %}
{% endmacro %}

{% macro snowflake__is_data_metric_function() %}
    {{ return(model.config.get('is_data_metric_function', false) is sameas true) }}
{% endmacro %}

{#
    A DMF built after the models that use it attaches itself to them: a name in config
    is not a dbt dependency, so dbt may build those models first, and they skip a DMF
    that does not exist yet. Only one-part names in the DMF's own schema are matched.
#}
{% macro snowflake__attach_data_metric_function_to_models(target_relation) %}
    {% for relation, desired in adapter.models_using_data_metric_function(graph.nodes.values() | list, target_relation) %}
        {% set existing_relation = adapter.get_relation(
            database=relation.database, schema=relation.schema, identifier=relation.identifier
        ) %}
        {% if existing_relation is not none %}
            {% set relation = existing_relation %}
            {% set current_state = adapter.describe_data_metrics(relation, desired) %}
            {% set current = adapter.data_metric_function_keys(current_state) %}
            {% for association in desired.functions if association.key not in current %}
                {{ log("Attaching data metric function " ~ target_relation ~ " to " ~ relation.render(), info=True) }}
                {% call statement('attach_data_metric_function') %}
                    {{ snowflake__get_add_data_metric_function_sql(relation, association) }}
                {% endcall %}
            {% endfor %}
        {% endif %}
    {% endfor %}
{% endmacro %}

{% macro snowflake__function_execute_build_sql(build_sql, existing_relation, target_relation) %}
    {{ default__function_execute_build_sql(build_sql, existing_relation, target_relation) }}
    {% if snowflake__is_data_metric_function() %}
        {{ snowflake__attach_data_metric_function_to_models(target_relation) }}
    {% endif %}
{% endmacro %}

{% macro snowflake__data_metric_function_sql(target_relation) %}
    {% if model.config.get('volatility') is not none %}
        {% do exceptions.warn(
            "`volatility` is ignored for data metric function `" ~ model.name ~ "`."
        ) %}
    {% endif %}
    CREATE OR ALTER DATA METRIC FUNCTION {{ target_relation.render() }} ({{ formatted_scalar_function_args_sql() }})
    RETURNS {{ model.returns.data_type }}
    AS
    {{ scalar_function_body_sql() }}
{% endmacro %}
