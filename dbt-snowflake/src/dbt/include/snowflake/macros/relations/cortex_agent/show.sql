{% macro snowflake__show_cortex_agent(relation) %}
    {% set query %}
        show agents like {{ dbt.string_literal(dbt.escape_single_quotes(relation.identifier | replace('\\', '\\\\'))) }}
        in schema {{ relation.include(identifier=False) }}
    {% endset %}
    {{ return(run_query(query)) }}
{% endmacro %}
