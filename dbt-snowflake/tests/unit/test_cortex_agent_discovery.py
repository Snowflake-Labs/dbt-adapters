from unittest import mock

import agate
import pytest

from dbt.adapters.snowflake.impl import SnowflakeAdapter
from dbt.adapters.snowflake.relation import SnowflakeRelation


@pytest.mark.parametrize(
    "identifier,quoted,names,expected",
    [
        ("support_agent", False, ["SUPPORT_AGENT"], "SUPPORT_AGENT"),
        ("SupportAgent", True, ["SupportAgent"], "SupportAgent"),
        ("SupportAgent", True, ["SUPPORTAGENT"], None),
        ("support_agent", False, ["SUPPORTXAGENT"], None),
        ("support_agent", False, [], None),
        ("support_agent", False, ["SUPPORTXAGENT", "SUPPORT_AGENT"], "SUPPORT_AGENT"),
    ],
)
def test_agent_discovery(identifier, quoted, names, expected):
    adapter = mock.Mock(spec=SnowflakeAdapter)
    adapter.Relation = SnowflakeRelation
    adapter.execute_macro.return_value = agate.Table(
        [[name] for name in names], ["NAME"], column_types=[agate.Text()]
    )
    adapter.get_relation.return_value = SnowflakeRelation.create(
        database="db", schema="sch", identifier=identifier, type=SnowflakeRelation.Table
    )
    relation = SnowflakeRelation.create(
        database="db", schema="sch", identifier=identifier, quote_policy={"identifier": quoted}
    )
    result = SnowflakeAdapter.get_cortex_agent(adapter, relation)
    adapter.get_relation.assert_not_called()
    adapter.execute_macro.assert_called_once_with(
        "snowflake__show_cortex_agent", kwargs={"relation": relation.as_case_sensitive()}
    )
    if expected is None:
        assert result is None
    else:
        assert result.identifier == expected
        assert result.is_cortex_agent
