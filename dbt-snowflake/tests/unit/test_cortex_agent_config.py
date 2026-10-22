from types import SimpleNamespace

from dbt.contracts.graph.model_config import ModelConfig

from dbt.adapters.snowflake.relation import SnowflakeRelation
from dbt.adapters.snowflake.relation_configs import SnowflakeCortexAgentConfig


def agent_config(**values):
    model = SimpleNamespace(
        identifier="support_agent",
        description="From schema.yml",
        config=ModelConfig.from_dict({"materialized": "cortex_agent", **values}),
    )
    return SnowflakeRelation.from_config(model)


def test_model_config_preserves_profile_and_boolean_values():
    profile = {"display_name": "Support Bot", "avatar": "support.png", "color": "blue"}
    agent = agent_config(profile=profile, secure=False, copy_grants=True, comment="")
    assert isinstance(agent, SnowflakeCortexAgentConfig)
    assert agent.profile == profile
    assert agent.secure is False
    assert agent.copy_grants is True
    assert agent.comment == ""


def test_metadata_defaults_and_unset_secure():
    agent = agent_config()
    assert agent.profile == {"display_name": "support_agent"}
    assert agent.comment == "From schema.yml"
    assert agent.secure is None
    assert agent.copy_grants is False
    assert agent.role is None


def test_legacy_meta_fallback_and_role():
    agent = agent_config(
        meta={"display_name": "Legacy Bot", "comment": "Legacy comment", "role": "OWNER"}
    )
    assert agent.profile == {"display_name": "Legacy Bot"}
    assert agent.comment == "Legacy comment"
    assert agent.role == "OWNER"


def test_explicit_top_level_metadata_wins():
    agent = agent_config(
        profile={}, comment="", meta={"display_name": "Legacy Bot", "comment": "Legacy comment"}
    )
    assert agent.profile == {}
    assert agent.comment == ""
