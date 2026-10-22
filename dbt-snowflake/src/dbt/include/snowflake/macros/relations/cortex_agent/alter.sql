{% macro snowflake__get_alter_cortex_agent_sql(relation, sql) %}
    alter agent {{ relation }} modify live version set specification =
$$
{{ sql }}
$$
{% endmacro %}


{% macro snowflake__get_alter_cortex_agent_properties_sql(agent, relation) %}
    alter agent {{ relation }} set
    {{ snowflake__cortex_agent_properties_sql(agent) }}
{% endmacro %}
