{% materialization cortex_agent, adapter='snowflake' %}

    {% set target_relation = this.incorporate(type=this.CortexAgent) %}

    {% if sql | trim == '' %}
        {% do exceptions.raise_compiler_error("Cortex Agent model " ~ this ~ " needs a non-empty agent specification as its body.") %}
    {% endif %}
    {% if '$$' in sql %}
        {% do exceptions.raise_compiler_error("Cortex Agent model " ~ this ~ ": the agent specification must not contain `$$`, which ends the dollar-quoted FROM SPECIFICATION body.") %}
    {% endif %}

    {% set display_name = config.meta_get('display_name') or this.identifier %}
    {% set comment = config.meta_get('comment') or model.description %}
    {% set role = config.meta_get('role') %}

    {% set query_tag = set_query_tag() %}

    {{ run_hooks(pre_hooks) }}

    {% if role %}
        {% set original_role = run_query('select current_role()').rows[0][0] %}
        {% call statement('use_role') -%}
            use role {{ role }}
        {%- endcall %}
    {% endif %}

    {% call statement('main') -%}
        create or alter agent {{ target_relation }}
        {%- if comment %}
        comment = '{{ cortex_agent_string_literal(comment) }}'
        {%- endif %}
        {%- if display_name %}
        profile = '{{ cortex_agent_string_literal({'display_name': display_name} | tojson) }}'
        {%- endif %}
        from specification
$$
{{ sql }}
$$
    {%- endcall %}

    {{ run_hooks(post_hooks) }}

    {# CREATE OR ALTER retains grants; the agent exists after the main statement. #}
    {% do apply_grants(target_relation, config.get('grants'), should_revoke=True) %}

    {% if role %}
        {% call statement('restore_role') -%}
            use role {{ adapter.quote(original_role) }}
        {%- endcall %}
    {% endif %}

    {% do unset_query_tag(query_tag) %}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}


{#- Body of a single-quoted Snowflake string literal. Backslashes are doubled too, since
    Snowflake reads backslash escapes in these literals (tojson output has `\"`). -#}
{%- macro cortex_agent_string_literal(value) -%}
    {{- value | string | replace('\\', '\\\\') | replace("'", "''") -}}
{%- endmacro -%}
