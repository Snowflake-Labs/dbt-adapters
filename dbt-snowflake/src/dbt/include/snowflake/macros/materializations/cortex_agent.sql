{% materialization cortex_agent, adapter='snowflake' %}

    {% set target_relation = this.incorporate(type=this.CortexAgent) %}

    {% if sql | trim == '' %}
        {% do exceptions.raise_compiler_error("Cortex Agent model " ~ this ~ " needs a non-empty agent specification as its body.") %}
    {% endif %}
    {% if '$$' in sql %}
        {% do exceptions.raise_compiler_error("Cortex Agent model " ~ this ~ ": the agent specification must not contain `$$`, which ends the dollar-quoted FROM SPECIFICATION body.") %}
    {% endif %}

    {% set agent = target_relation.from_config(config.model) %}
    {% set full_refresh_mode = should_full_refresh() %}
    {% set role = agent.role %}

    {% set query_tag = set_query_tag() %}

    {{ run_hooks(pre_hooks) }}

    {% if role %}
        {% set original_role = run_query('select current_role()').rows[0][0] %}
        {% call statement('use_role') -%}
            use role {{ role }}
        {%- endcall %}
    {% endif %}

    {% set existing_relation = adapter.get_cortex_agent(target_relation) %}

    {% call statement('main') -%}
        {% if existing_relation is none or full_refresh_mode %}
            {{ snowflake__get_create_cortex_agent_sql(agent, target_relation, sql, full_refresh_mode) }}
        {% else %}
            {{ snowflake__get_alter_cortex_agent_sql(target_relation, sql) }}
        {% endif %}
    {%- endcall %}

    {% if existing_relation is not none and not full_refresh_mode %}
        {% call statement('agent_properties') -%}
            {{ snowflake__get_alter_cortex_agent_properties_sql(agent, target_relation) }}
        {%- endcall %}
    {% endif %}

    {{ run_hooks(post_hooks) }}

    {% do apply_grants(target_relation, config.get('grants'), should_revoke=should_revoke(existing_relation, full_refresh_mode=full_refresh_mode)) %}

    {% if role %}
        {% call statement('restore_role') -%}
            use role {{ adapter.quote(original_role) }}
        {%- endcall %}
    {% endif %}

    {% do unset_query_tag(query_tag) %}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
