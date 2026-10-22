{% macro snowflake__get_create_cortex_agent_sql(agent, relation, sql, full_refresh_mode=False) %}
    create {% if full_refresh_mode %}or replace {% endif %}agent {{ relation }}
    {% if full_refresh_mode and agent.copy_grants %}copy grants{% endif %}
    {{ snowflake__cortex_agent_properties_sql(agent) }}
    from specification
$$
{{ sql }}
$$
{% endmacro %}


{% macro snowflake__cortex_agent_properties_sql(agent) %}
    {{ optional('secure', agent.secure | string | upper if agent.secure is not none else none) }}
    {% if agent.comment is not none %}
        {{ optional('comment', dbt.escape_single_quotes(agent.comment | replace('\\', '\\\\')), "'") }}
    {% endif %}
    {{ optional('profile', dbt.escape_single_quotes(agent.profile | tojson | replace('\\', '\\\\')), "'") }}
{% endmacro %}
