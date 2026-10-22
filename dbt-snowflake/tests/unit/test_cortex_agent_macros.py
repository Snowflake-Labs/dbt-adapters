import json
import os
import re
import unittest
from unittest import mock
from types import SimpleNamespace

import pytest
from dbt_common.exceptions import CompilationError

from dbt_common.clients.jinja import MaterializationExtension
from jinja2 import Environment, FileSystemLoader

from dbt.adapters.snowflake.relation import SnowflakeRelation
from dbt.include.global_project import PACKAGE_PATH as GLOBAL_PROJECT_PATH

MACROS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../src/dbt/include/snowflake/macros")
)

SPEC = (
    "models:\n  orchestration: auto\ninstructions:\n  response: 'Answer briefly.'\n"
    "tools:\n  - tool_spec:\n      type: cortex_search\n      name: docs\n"
    "tool_resources:\n  docs:\n    search_service: test_db.test_schema.docs_search"
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
        self.config_values = {
            "grants": self.grants,
            "meta": self.meta,
            "materialized": "cortex_agent",
        }
        self.context["config"].get = self.config_values.get
        self.context["model"] = SimpleNamespace(
            identifier="test_agent",
            description="",
            config=SimpleNamespace(get=self.config_values.get, materialized="cortex_agent"),
        )
        self.context["config"].model = self.context["model"]
        self.context["adapter"].get_cortex_agent.return_value = None
        self.context["optional"] = lambda *args: self.module("utils/optional.sql").optional(*args)
        self.context["snowflake__cortex_agent_properties_sql"] = lambda *args: self.module(
            "relations/cortex_agent/create.sql"
        ).snowflake__cortex_agent_properties_sql(*args)
        for name, filename in (
            ("snowflake__get_create_cortex_agent_sql", "create.sql"),
            ("snowflake__get_alter_cortex_agent_sql", "alter.sql"),
            ("snowflake__get_alter_cortex_agent_properties_sql", "alter.sql"),
        ):
            self.context[name] = lambda *args, name=name, filename=filename: getattr(
                self.module("relations/cortex_agent/" + filename), name
            )(*args)

        self.context["adapter"].quote = lambda value: '"' + value.replace('"', '""') + '"'
        with open(os.path.join(GLOBAL_PROJECT_PATH, "macros/utils/literal.sql")) as f:
            literal_macros = self.jinja_env.from_string(f.read()).make_module(self.context)
        with open(os.path.join(GLOBAL_PROJECT_PATH, "macros/utils/escape_single_quotes.sql")) as f:
            escape_macros = self.jinja_env.from_string(f.read()).make_module(self.context)
        dispatched_macros = {
            "string_literal": literal_macros.default__string_literal,
            "escape_single_quotes": self.module(
                "utils/escape_single_quotes.sql"
            ).snowflake__escape_single_quotes,
        }
        self.context["adapter"].dispatch.side_effect = lambda name, package: dispatched_macros[
            name
        ]

        def invoke_macro(macro, value):
            try:
                return macro(value)
            except _MacroReturn as returned:
                return returned.value

        self.context["dbt"] = SimpleNamespace(
            string_literal=lambda value: invoke_macro(literal_macros.string_literal, value),
            escape_single_quotes=lambda value: invoke_macro(
                escape_macros.escape_single_quotes, value
            ),
        )

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
            f"create agent {RELATION} "
            """profile = '{"display_name": "test_agent"}' """
            f"from specification $$ {SPEC} $$"
        )
        assert [r.render() for r in result["relations"]] == [RELATION]
        assert result["relations"][0].is_cortex_agent

    def test_spec_is_passed_verbatim_in_dollar_quotes(self):
        self.run_materialization()
        assert f"from specification\n$$\n{SPEC}\n$$" in self.main_sql()

    def test_ddl_with_comment_and_display_name(self):
        self.meta.update({"comment": "Support agent", "display_name": "Support Bot"})
        self.run_materialization()
        assert _squash(self.main_sql()) == _squash(
            f"create agent {RELATION} comment = 'Support agent' "
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
        assert _snowflake_literal(self.main_sql(), "comment") == "It's Bob's agent"
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
        assert self.statements[1][1].lstrip().startswith("create agent")
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

    def test_full_refresh_replaces_agent(self):
        self.context["should_full_refresh"].return_value = True
        self.run_materialization()
        sql = self.main_sql()
        assert sql.lstrip().startswith(f"create or replace agent {RELATION}")

    def test_grants_applied_to_agent(self):
        existing = _relation(type=SnowflakeRelation.CortexAgent)
        self.context["adapter"].get_cortex_agent.return_value = existing
        self.context["should_revoke"].return_value = True
        self.run_materialization()
        self.context["adapter"].get_cortex_agent.assert_called_once()
        discovered = self.context["adapter"].get_cortex_agent.call_args.args[0]
        assert discovered.is_cortex_agent
        assert discovered.render() == RELATION
        self.context["should_revoke"].assert_called_once_with(existing, full_refresh_mode=False)
        self.context["load_cached_relation"].assert_not_called()
        relation, grant_config = self.context["apply_grants"].call_args.args
        assert relation.is_cortex_agent
        assert grant_config == self.grants
        assert self.context["apply_grants"].call_args.kwargs == {"should_revoke": True}

    def test_existing_agent_updates_specification_and_metadata(self):
        self.context["adapter"].get_cortex_agent.return_value = _relation(
            type=SnowflakeRelation.CortexAgent
        )
        self.config_values.update(
            secure=False,
            comment="",
            profile={"display_name": "Support Bot", "avatar": "support.png", "color": "blue"},
        )
        self.run_materialization()
        assert [name for name, _ in self.statements] == ["main", "agent_properties"]
        assert _squash(self.statements[0][1]) == _squash(
            f"alter agent {RELATION} modify live version set specification = $$ {SPEC} $$"
        )
        properties = self.statements[1][1]
        assert properties.lstrip().startswith(f"alter agent {RELATION} set")
        assert "secure FALSE" not in properties
        assert "secure = FALSE" in properties
        assert _snowflake_literal(properties, "comment") == ""
        assert (
            json.loads(_snowflake_literal(properties, "profile")) == self.config_values["profile"]
        )
        assert "copy grants" not in properties

    def test_discovery_runs_under_selected_role(self):
        self.meta["role"] = "agent_owner"

        def discover(relation):
            assert [name for name, _ in self.statements] == ["use_role"]
            return None

        self.context["adapter"].get_cortex_agent.side_effect = discover
        self.run_materialization()

    def test_show_agent_quotes_identifier_and_schema(self):
        relation = SnowflakeRelation.create(
            database="Test DB",
            schema="Test Schema",
            identifier="Bob's Agent",
            quote_policy={"database": True, "schema": True, "identifier": True},
        )
        with self.assertRaises(_MacroReturn):
            self.module("relations/cortex_agent/show.sql").snowflake__show_cortex_agent(relation)
        query = self.context["run_query"].call_args.args[0]
        assert _squash(query) == (
            "show agents like 'Bob\\'s Agent' " 'in schema "Test DB"."Test Schema"'
        )

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


@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("full_refresh", [False, True])
@pytest.mark.parametrize("copy_grants", [False, True])
def test_agent_lifecycle_and_grant_reconciliation(existing, full_refresh, copy_grants):
    test = TestCortexAgentMaterialization()
    test.setUp()
    relation = _relation(type=SnowflakeRelation.CortexAgent) if existing else None
    test.context["adapter"].get_cortex_agent.return_value = relation
    test.context["should_full_refresh"].return_value = full_refresh
    test.config_values["copy_grants"] = copy_grants
    with open(os.path.join(GLOBAL_PROJECT_PATH, "macros/adapters/apply_grants.sql")) as f:
        shared = test.jinja_env.from_string(f.read()).make_module(
            {
                "return": _raise_return,
                "adapter": SimpleNamespace(dispatch=lambda name, package: lambda: copy_grants),
            }
        )

    def should_revoke(*args, **kwargs):
        try:
            shared.should_revoke(*args, **kwargs)
        except _MacroReturn as returned:
            return returned.value

    test.context["should_revoke"] = should_revoke
    test.run_materialization()
    main = _squash(test.statements[0][1])
    if full_refresh:
        assert main.startswith(f"create or replace agent {RELATION}")
    elif existing:
        assert main.startswith(f"alter agent {RELATION} modify live version")
        assert [name for name, _ in test.statements] == ["main", "agent_properties"]
    else:
        assert main.startswith(f"create agent {RELATION}")
    assert ("copy grants" in main) == (full_refresh and copy_grants)
    assert "create or alter" not in main
    assert test.context["apply_grants"].call_args.kwargs == {
        "should_revoke": existing and (not full_refresh or copy_grants)
    }


@pytest.mark.parametrize("secure", [False, True])
def test_secure_option_is_rendered_on_create(secure):
    test = TestCortexAgentMaterialization()
    test.setUp()
    test.config_values["secure"] = secure
    test.run_materialization()
    assert f"secure = {str(secure).upper()}" in test.main_sql()


@pytest.mark.parametrize(
    "name,value",
    [
        ("secure", "true"),
        ("secure", 0),
        ("copy_grants", "false"),
        ("copy_grants", 1),
        ("profile", "Support Bot"),
        ("profile", []),
        ("profile", {"color": 123}),
        ("comment", 123),
    ],
)
def test_invalid_agent_config_fails_before_hooks(name, value):
    test = TestCortexAgentMaterialization()
    test.setUp()
    test.config_values[name] = value
    with pytest.raises(CompilationError, match=name):
        test.run_materialization()
    assert test.statements == []
    test.context["run_hooks"].assert_not_called()
    test.context["set_query_tag"].assert_not_called()


def test_top_level_metadata_overrides_legacy_meta_and_preserves_empty_values():
    test = TestCortexAgentMaterialization()
    test.setUp()
    test.context["model"].description = "From schema.yml"
    test.meta.update(comment="From meta", display_name="Legacy display name")
    test.config_values.update(comment="", profile={})
    test.run_materialization()
    assert _snowflake_literal(test.main_sql(), "comment") == ""
    assert json.loads(_snowflake_literal(test.main_sql(), "profile")) == {}
