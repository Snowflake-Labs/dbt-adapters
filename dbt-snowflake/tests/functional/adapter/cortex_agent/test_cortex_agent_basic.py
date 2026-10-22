"""Live-account tests for the `cortex_agent` materialization.

They need a Snowflake account with Cortex Agents and Cortex Search enabled, and
`SNOWFLAKE_TEST_WAREHOUSE` set to a warehouse the search service can refresh with.
"""

import json

import pytest

from dbt.tests.util import (
    assert_message_in_logs,
    get_connection,
    get_manifest,
    relation_from_name,
    run_dbt,
    run_dbt_and_capture,
)

from tests.functional.utils import update_model


SEED = """
doc_id,doc_name,body,region
1,password,how to reset a password,us
2,billing,billing cycle explained,eu
3,export,exporting reports to csv,us
""".strip()


SERVICE = """
{{ config(
    materialized='cortex_search_service',
    search_column='body',
    attributes=['region'],
    snowflake_warehouse=target.warehouse,
    target_lag='1 hour'
) }}
select doc_id, doc_name, body, region from {{ ref('support_docs') }}
"""

# The model body is the agent spec. `ref()` resolves the service name and orders the
# agent after the service.
AGENT = """
{{ config(
    materialized='cortex_agent',
    profile={'display_name': "Bob's Support Bot", 'avatar': 'support.png', 'color': 'blue'},
    comment="Bob's support agent",
    secure=true,
    copy_grants=true,
) }}
models:
  orchestration: auto
instructions:
  response: "Answer from the support articles only."
tools:
  - tool_spec:
      type: cortex_search
      name: support_docs
      description: "Support articles"
tool_resources:
  support_docs:
    search_service: "{{ ref('support_search') }}"
    max_results: 5
"""

AGENT_SPEC_CHANGE = AGENT.replace(
    "Answer from the support articles only.", "Answer briefly, from the support articles only."
)

# No meta: the comment comes from the schema.yml description, the display name from
# the model name.
PLAIN_AGENT = """
{{ config(materialized='cortex_agent') }}
models:
  orchestration: auto
instructions:
  response: "Say hello."
"""

SCHEMA_YML = """
version: 2
models:
  - name: plain_agent
    description: "Described in schema.yml"
"""


def _show(project, sql):
    with get_connection(project.adapter):
        _, table = project.adapter.execute(sql, fetch=True)
    return table.rename(column_names=[c.lower() for c in table.column_names])


def show_agent(project, name):
    relation = relation_from_name(project.adapter, name)
    table = _show(
        project,
        f"show agents like '{relation.identifier}' in schema {relation.database}.{relation.schema}",
    )
    rows = [row for row in table if row["name"].upper() == relation.identifier.upper()]
    assert len(rows) == 1, f"expected one agent named {relation.identifier}, got {rows}"
    return rows[0]


def live_versions(project, name):
    relation = relation_from_name(project.adapter, name)
    table = _show(project, f"show versions in agent {relation}")
    # The live version may show an empty `name`; its spec path always ends in /versions/live/.
    return [
        row
        for row in table
        if (row["name"] or "").upper() == "LIVE"
        or "/versions/live/" in (row["spec_file_path"] or "").lower()
    ]


def live_spec(project, name):
    relation = relation_from_name(project.adapter, name)
    table = _show(project, f"describe agent {relation}")
    assert len(table.rows) == 1
    return table.rows[0]["agent_spec"]


class CortexAgentBase:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"support_docs.csv": SEED}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "support_search.sql": SERVICE,
            "support_agent.sql": AGENT,
            "plain_agent.sql": PLAIN_AGENT,
            "schema.yml": SCHEMA_YML,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield


class TestCortexAgentBasic(CortexAgentBase):
    def test_create(self, project):
        assert show_agent(project, "support_agent")["created_on"] is not None
        assert live_versions(project, "support_agent")

    def test_ref_to_search_service(self, project):
        manifest = get_manifest(project.project_root)
        agent_node = manifest.nodes["model.test.support_agent"]
        assert "model.test.support_search" in agent_node.depends_on.nodes

        service = relation_from_name(project.adapter, "support_search")
        assert service.identifier.upper() in live_spec(project, "support_agent").upper()

    def test_comment_and_profile_from_config(self, project):
        agent = show_agent(project, "support_agent")
        assert agent["comment"] == "Bob's support agent"
        assert json.loads(agent["profile"]) == {
            "display_name": "Bob's Support Bot",
            "avatar": "support.png",
            "color": "blue",
        }

    def test_comment_and_profile_defaults(self, project):
        agent = show_agent(project, "plain_agent")
        assert agent["comment"] == "Described in schema.yml"
        assert json.loads(agent["profile"])["display_name"].lower() == "plain_agent"


class TestCortexAgentSpecChange(CortexAgentBase):
    def test_spec_change_alters_in_place(self, project):
        created_on = show_agent(project, "support_agent")["created_on"]

        update_model(project, "support_agent", AGENT_SPEC_CHANGE)
        _, logs = run_dbt_and_capture(["--debug", "run", "--select", "support_agent"])
        assert_message_in_logs("modify live version set specification", logs)

        assert show_agent(project, "support_agent")["created_on"] == created_on
        assert live_versions(project, "support_agent")
        assert "Answer briefly, from the support articles only." in live_spec(
            project, "support_agent"
        )

    def test_full_refresh_replaces_with_copy_grants(self, project):
        created_on = show_agent(project, "support_agent")["created_on"]
        _, logs = run_dbt_and_capture(
            ["--debug", "run", "--select", "support_agent", "--full-refresh"]
        )
        assert_message_in_logs("create or replace agent", logs)
        assert_message_in_logs("copy grants", logs)
        assert show_agent(project, "support_agent")["created_on"] != created_on
