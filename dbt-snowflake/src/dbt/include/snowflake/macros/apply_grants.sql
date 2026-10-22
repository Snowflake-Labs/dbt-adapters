{% macro snowflake__copy_grants() %}
    {% set copy_grants = config.get('copy_grants', False) %}
    {{ return(copy_grants) }}
{% endmacro %}

{%- macro snowflake__support_multiple_grantees_per_dcl_statement() -%}
    {{ return(False) }}
{%- endmacro -%}


{#- Cortex Search services and Cortex Agents need their object type spelled out in
    grant DCL; the defaults' bare `on <name>` resolves as a table. Everything else
    keeps the defaults. The helper renders the object type (empty for everything else). -#}
{%- macro snowflake__grant_object_type(relation) -%}
    {%- if relation.is_cortex_search_service -%}
        cortex search service
    {%- elif relation.is_cortex_agent -%}
        agent
    {%- endif -%}
{%- endmacro -%}


{% macro snowflake__get_show_grant_sql(relation) %}
    {%- set object_type = snowflake__grant_object_type(relation) | trim -%}
    {%- if object_type -%}
        show grants on {{ object_type }} {{ relation.render() }}
    {%- else -%}
        {{ default__get_show_grant_sql(relation) }}
    {%- endif -%}
{% endmacro %}


{%- macro snowflake__get_grant_sql(relation, privilege, grantees) -%}
    {%- set object_type = snowflake__grant_object_type(relation) | trim -%}
    {%- if object_type -%}
        grant {{ privilege }} on {{ object_type }} {{ relation.render() }} to {{ grantees | join(', ') }}
    {%- else -%}
        {{ default__get_grant_sql(relation, privilege, grantees) }}
    {%- endif -%}
{%- endmacro -%}


{%- macro snowflake__get_revoke_sql(relation, privilege, grantees) -%}
    {%- set object_type = snowflake__grant_object_type(relation) | trim -%}
    {%- if object_type -%}
        {%- set quoted_grantees = [] -%}
        {%- for grantee in grantees -%}
            {%- do quoted_grantees.append(adapter.quote(grantee)) -%}
        {%- endfor -%}
        revoke {{ privilege }} on {{ object_type }} {{ relation.render() }} from {{ quoted_grantees | join(', ') }}
    {%- else -%}
        {{ default__get_revoke_sql(relation, privilege, grantees) }}
    {%- endif -%}
{%- endmacro -%}
