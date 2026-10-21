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
