import json
import os
import re
import unittest
from unittest import mock

from dbt_common.clients.jinja import MaterializationExtension
from jinja2 import Environment, FileSystemLoader

from dbt.adapters.snowflake.relation import SnowflakeRelation

MACROS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../src/dbt/include/snowflake/macros")
)

SPEC = (
    "models:\n  orchestration: auto\ninstructions:\n  response: 'Answer briefly.'\n"
    "tools:\n  - tool_spec:\n      type: cortex_search\n      name: docs\n"
    "tool_resources:\n  docs:\n    name: test_db.test_schema.docs_search"
)
RELATION = "test_db.test_schema.test_agent"


class _MacroReturn(Exception):
    """dbt's `return()` raises to unwind the macro; plain Jinja's would just render."""

    def __init__(self, value):
        self.value = value


class _CompilerError(Exception):
    pass


def _raise_return(value):
    raise _MacroReturn(value)


def _raise_compiler_error(msg):
    raise _CompilerError(msg)


def _squash(rendered):
    return re.sub(r"\s+", " ", rendered.strip())


def _relation(**kwargs):
    return SnowflakeRelation.create(
        database="test_db", schema="test_schema", identifier="test_agent", **kwargs
    )


def _snowflake_literal(sql, keyword):
    """The value of `<keyword> = '<literal>'` in `sql`, with Snowflake's escapes undone."""
    match = re.search(rf"{keyword} = '((?:[^'\\]|''|\\.)*)'", sql)
    assert match, f"no {keyword} literal in {sql!r}"
    return re.sub(r"''|\\(.)", lambda m: m.group(1) or "'", match.group(1))


class _MacroTestBase(unittest.TestCase):
    def setUp(self):
        self.jinja_env = Environment(
            loader=FileSystemLoader(MACROS_DIR),
            extensions=["jinja2.ext.do", MaterializationExtension],
        )
        self.context = {
            "adapter": mock.Mock(),
            "config": mock.Mock(),
            "return": _raise_return,
        }

    def module(self, template_filename):
        return self.jinja_env.get_template(template_filename, globals=self.context).module


class TestCortexAgentMaterialization(_MacroTestBase):
    def setUp(self):
        super().setUp()
        self.statements = []
        self.grants = {"usage": ["role_a"]}
        self.meta = {}
        self.context.update(
            {
                "this": _relation(),
                "sql": SPEC,
                "model": mock.Mock(description=""),
                "pre_hooks": [],
                "post_hooks": [],
                "exceptions": mock.Mock(raise_compiler_error=_raise_compiler_error),
                "set_query_tag": mock.Mock(return_value="original_tag"),
                "unset_query_tag": mock.Mock(),
                "should_full_refresh": mock.Mock(return_value=False),
                "load_cached_relation": mock.Mock(return_value=None),
                "run_hooks": mock.Mock(return_value=""),
                "statement": self.statement,
                "run_query": mock.Mock(return_value=mock.Mock(rows=[["original_role"]])),
                "should_revoke": mock.Mock(return_value=False),
                "apply_grants": mock.Mock(),
            }
        )
        self.context["config"].get = {"grants": self.grants}.get
        self.context["adapter"].quote = lambda value: '"' + value.replace('"', '""') + '"'
        self.context["config"].meta_get = lambda key, default=None: self.meta.get(key, default)

    def statement(self, name, caller=None, **kwargs):
        self.statements.append((name, caller()))
        return ""

    def run_materialization(self):
        module = self.module("materializations/cortex_agent.sql")
        try:
            module.dbt_macro__materialization_cortex_agent_snowflake()
        except _MacroReturn as returned:
            return returned.value
        raise AssertionError("materialization did not return()")

    def main_sql(self):
        assert [name for name, _ in self.statements] == ["main"]
        return self.statements[0][1]

    def assert_raises_before_any_statement(self, message):
        with self.assertRaisesRegex(_CompilerError, message):
            self.run_materialization()
        assert self.statements == []
        self.context["run_hooks"].assert_not_called()
        self.context["set_query_tag"].assert_not_called()
        self.context["apply_grants"].assert_not_called()

    def test_ddl_without_comment(self):
        result = self.run_materialization()
        assert _squash(self.main_sql()) == _squash(
            f"create or alter agent {RELATION} "
            """profile = '{"display_name": "test_agent"}' """
            f"from specification $$ {SPEC} $$"
        )
        assert [r.render() for r in result["relations"]] == [RELATION]
        assert result["relations"][0].is_cortex_agent

    def test_spec_is_passed_verbatim_in_dollar_quotes(self):
        self.run_materialization()
        assert self.main_sql().endswith(f"from specification\n$$\n{SPEC}\n$$")

    def test_ddl_with_comment_and_display_name(self):
        self.meta.update({"comment": "Support agent", "display_name": "Support Bot"})
        self.run_materialization()
        assert _squash(self.main_sql()) == _squash(
            f"create or alter agent {RELATION} comment = 'Support agent' "
            """profile = '{"display_name": "Support Bot"}' """
            f"from specification $$ {SPEC} $$"
        )

    def test_comment_falls_back_to_model_description(self):
        self.context["model"].description = "From schema.yml"
        self.run_materialization()
        assert _snowflake_literal(self.main_sql(), "comment") == "From schema.yml"

    def test_meta_comment_wins_over_model_description(self):
        self.context["model"].description = "From schema.yml"
        self.meta["comment"] = "From meta"
        self.run_materialization()
        assert _snowflake_literal(self.main_sql(), "comment") == "From meta"

    def test_display_name_defaults_to_identifier(self):
        self.run_materialization()
        profile = json.loads(_snowflake_literal(self.main_sql(), "profile"))
        assert profile == {"display_name": "test_agent"}

    def test_comment_quotes_are_escaped(self):
        self.meta["comment"] = "It's Bob's agent"
        self.run_materialization()
        assert "comment = 'It''s Bob''s agent'" in self.main_sql()
        assert _snowflake_literal(self.main_sql(), "comment") == "It's Bob's agent"

    def test_comment_backslashes_are_escaped(self):
        self.meta["comment"] = "ends with a backslash \\"
        self.run_materialization()
        assert "comment = 'ends with a backslash \\\\'" in self.main_sql()
        assert _snowflake_literal(self.main_sql(), "comment") == "ends with a backslash \\"

    def test_profile_is_valid_json_with_awkward_display_name(self):
        display_name = 'Bob\'s "support" <bot> \\ agent'
        self.meta["display_name"] = display_name
        self.run_materialization()
        literal = re.search(r"profile = '([^\n]*)'\n", self.main_sql()).group(1)
        # The literal body is a single-quoted Snowflake string: no lone quote ends it early.
        assert "'" not in literal.replace("''", "")
        profile = json.loads(_snowflake_literal(self.main_sql(), "profile"))
        assert profile == {"display_name": display_name}

    def test_dollar_quote_in_spec_raises_before_any_statement(self):
        self.context["sql"] = SPEC + "\n# $$ oops"
        self.assert_raises_before_any_statement(r"must not contain `\$\$`")

    def test_empty_spec_raises_before_any_statement(self):
        for empty in ("", "  \n\t\n"):
            self.context["sql"] = empty
            self.assert_raises_before_any_statement("non-empty agent specification")

    def test_role_runs_use_role_first(self):
        self.meta["role"] = "agent_owner"
        self.run_materialization()
        assert [name for name, _ in self.statements] == ["use_role", "main", "restore_role"]
        assert _squash(self.statements[0][1]) == "use role agent_owner"
        assert "use role" not in self.statements[1][1]
        assert self.statements[1][1].lstrip().startswith("create or alter agent")
        assert _squash(self.statements[2][1]) == 'use role "original_role"'

    def test_restored_role_is_quoted(self):
        self.meta["role"] = "agent_owner"
        self.context["run_query"].return_value = mock.Mock(rows=[['Original "Role"']])
        self.run_materialization()
        assert _squash(self.statements[-1][1]) == 'use role "Original ""Role"""'

    def test_no_role_no_use_role(self):
        self.run_materialization()
        assert "use role" not in self.main_sql()
        self.context["run_query"].assert_not_called()

    def test_full_refresh_still_creates_or_alters(self):
        self.context["should_full_refresh"].return_value = True
        self.run_materialization()
        sql = self.main_sql()
        assert sql.lstrip().startswith(f"create or alter agent {RELATION}")
        assert "replace" not in sql

    def test_grants_applied_to_agent(self):
        existing = _relation(type=SnowflakeRelation.CortexAgent)
        self.context["load_cached_relation"].return_value = existing
        self.run_materialization()

        self.context["should_revoke"].assert_not_called()
        self.context["load_cached_relation"].assert_not_called()
        self.context["apply_grants"].assert_called_once()
        relation, grant_config = self.context["apply_grants"].call_args.args
        assert relation.is_cortex_agent
        assert grant_config == self.grants
        assert self.context["apply_grants"].call_args.kwargs == {"should_revoke": True}

    def test_grants_reconcile_without_cached_agent_even_on_full_refresh(self):
        for full_refresh in (False, True):
            with self.subTest(full_refresh=full_refresh):
                self.context["should_full_refresh"].return_value = full_refresh
                self.run_materialization()
                assert self.context["apply_grants"].call_args.kwargs == {"should_revoke": True}
        self.context["load_cached_relation"].assert_not_called()

    def test_grants_render_agent_grant_sql(self):
        """The relation handed to apply_grants renders the agent grant DCL."""
        self.run_materialization()
        relation, grant_config = self.context["apply_grants"].call_args.args
        grant_macros = self.module("apply_grants.sql")
        rendered = [
            _squash(grant_macros.snowflake__get_grant_sql(relation, privilege, [grantee]))
            for privilege, grantees in grant_config.items()
            for grantee in grantees
        ]
        assert rendered == [f"grant usage on agent {RELATION} to role_a"]

    def test_hooks_and_query_tag(self):
        def run_hooks(hooks):
            self.context["apply_grants"].assert_not_called()
            return ""

        self.context["run_hooks"].side_effect = run_hooks
        self.run_materialization()
        assert self.context["run_hooks"].call_args_list == [
            mock.call(self.context["pre_hooks"]),
            mock.call(self.context["post_hooks"]),
        ]
        self.context["adapter"].commit.assert_not_called()
        self.context["unset_query_tag"].assert_called_once_with("original_tag")


class TestCortexAgentDropMacros(_MacroTestBase):
    def test_drop(self):
        rendered = _squash(
            self.module("relations/cortex_agent/drop.sql").snowflake__get_drop_cortex_agent_sql(
                RELATION
            )
        )
        assert rendered == f"drop agent if exists {RELATION}"

    def test_drop_dispatch(self):
        self.context["snowflake__get_drop_cortex_agent_sql"] = mock.Mock(
            return_value="snowflake__get_drop_cortex_agent_sql"
        )
        relation = _relation(type=SnowflakeRelation.CortexAgent)
        rendered = self.module("relations/drop.sql").snowflake__get_drop_sql(relation)
        assert rendered.strip() == "snowflake__get_drop_cortex_agent_sql"


class TestCortexAgentGrantMacros(_MacroTestBase):
    def setUp(self):
        super().setUp()
        self.context["adapter"].quote = lambda value: f'"{value}"'
        for name in [
            "default__get_show_grant_sql",
            "default__get_grant_sql",
            "default__get_revoke_sql",
        ]:
            self.context[name] = mock.Mock(return_value=name)
        self.grants = self.module("apply_grants.sql")
        self.agent = _relation(type=SnowflakeRelation.CortexAgent)

    def test_show_grants(self):
        assert _squash(self.grants.snowflake__get_show_grant_sql(self.agent)) == (
            f"show grants on agent {RELATION}"
        )

    def test_grant(self):
        assert _squash(
            self.grants.snowflake__get_grant_sql(self.agent, "usage", ["r1", "r2"])
        ) == (f"grant usage on agent {RELATION} to r1, r2")

    def test_revoke_quotes_grantees(self):
        assert _squash(self.grants.snowflake__get_revoke_sql(self.agent, "usage", ["r1"])) == (
            f'revoke usage on agent {RELATION} from "r1"'
        )

    def test_search_service_still_uses_its_own_object_type(self):
        service = _relation(type=SnowflakeRelation.CortexSearchService)
        assert _squash(self.grants.snowflake__get_grant_sql(service, "usage", ["r"])) == (
            f"grant usage on cortex search service {RELATION} to r"
        )
