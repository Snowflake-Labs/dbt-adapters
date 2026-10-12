import os
import re
import unittest
from types import SimpleNamespace
from unittest import mock

from jinja2 import Environment, FileSystemLoader

MACROS_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "../../src/dbt/include/snowflake/macros")
)


class _MacroReturn(Exception):
    def __init__(self, value):
        self.value = value


def _raise_return(value):
    # dbt's `return()` exits the macro; plain jinja2 would keep rendering.
    raise _MacroReturn(value)


def _call(macro, *args):
    try:
        return macro(*args)
    except _MacroReturn as returned:
        return returned.value


def _association(function_sql, columns_sql, local_name=None):
    function = [SimpleNamespace(normalized=local_name or function_sql.split(".")[-1].upper())]
    return SimpleNamespace(
        function_sql=function_sql,
        columns_sql=columns_sql,
        is_local=local_name is not None,
        function=function,
    )


class _Relation:
    def __init__(self, relation_type, changeset=None, is_iceberg_format=False):
        self.type = relation_type
        self.is_iceberg_format = is_iceberg_format
        self.is_dynamic_table = relation_type == "dynamic_table"
        self._changeset = changeset
        self.changeset_calls = []

    def render(self):
        return "test_db.test_schema.test_model"

    def data_metrics_config_changeset(self, current_state, data_metrics):
        self.changeset_calls.append((current_state, data_metrics))
        return self._changeset


class _Changeset(SimpleNamespace):
    def __init__(self, schedule, drops, adds):
        super().__init__(schedule=schedule, drops=drops, adds=adds)

    @property
    def has_changes(self):
        return self.schedule is not None or bool(self.adds) or bool(self.drops)


class TestSnowflakeDataMetricsMacros(unittest.TestCase):
    def setUp(self):
        self.jinja_env = Environment(
            loader=FileSystemLoader(MACROS_DIR),
            extensions=["jinja2.ext.do"],
        )
        self.data_metrics = SimpleNamespace(is_configured=True)
        self.statements = []
        self.default_context = {
            "adapter": mock.Mock(),
            "log": lambda message, info=False: "",
            "return": _raise_return,
        }
        self.default_context["adapter"].describe_data_metrics = mock.Mock(return_value="state")
        self.default_context["adapter"].missing_local_data_metric_functions = mock.Mock(
            return_value=[]
        )

        statements = self.statements

        class _StatementCaller:
            # Emulates `{% call statement(...) %}`: the caller body is the SQL.
            def __call__(self, name, caller=None, **kwargs):
                statements.append(re.sub(r"\s+", " ", caller().strip()))
                return ""

        self.default_context["statement"] = _StatementCaller()
        self.template = self.jinja_env.get_template(
            "relations/data_metrics.sql", globals=self.default_context
        )

    def _changeset(self, schedule=None, drops=(), adds=()):
        return _Changeset(schedule, list(drops), list(adds))

    def _render_statements(self, relation, changeset):
        relation._changeset = changeset
        _call(self.template.module.snowflake__apply_data_metrics, relation, self.data_metrics)
        return self.statements

    def test_table_statements_in_order(self):
        changeset = self._changeset(
            schedule="5 MINUTE",
            drops=[_association('"SNOWFLAKE"."CORE"."NULL_COUNT"', '"STATUS"')],
            adds=[
                _association("snowflake.core.null_count", "amount"),
                _association("snowflake.core.row_count", ""),
            ],
        )
        assert self._render_statements(_Relation("table"), changeset) == [
            "alter table test_db.test_schema.test_model set data_metric_schedule = '5 MINUTE'",
            "alter table test_db.test_schema.test_model add data metric function snowflake.core.null_count on (amount)",
            "alter table test_db.test_schema.test_model add data metric function snowflake.core.row_count on ()",
            'alter table test_db.test_schema.test_model drop data metric function "SNOWFLAKE"."CORE"."NULL_COUNT" on ("STATUS")',
        ]

    def test_view_uses_alter_view(self):
        changeset = self._changeset(adds=[_association("snowflake.core.row_count", "")])
        assert self._render_statements(_Relation("view"), changeset) == [
            "alter view test_db.test_schema.test_model add data metric function snowflake.core.row_count on ()",
        ]

    def test_dynamic_table_uses_alter_dynamic_table(self):
        changeset = self._changeset(
            schedule="5 MINUTE",
            adds=[_association("snowflake.core.row_count", "")],
        )
        assert self._render_statements(_Relation("dynamic_table"), changeset) == [
            "alter dynamic table test_db.test_schema.test_model set data_metric_schedule = '5 MINUTE'",
            "alter dynamic table test_db.test_schema.test_model add data metric function snowflake.core.row_count on ()",
        ]

    def test_iceberg_table_uses_alter_iceberg_table(self):
        changeset = self._changeset(
            schedule="5 MINUTE",
            adds=[_association("snowflake.core.row_count", "")],
        )
        relation = _Relation("table", is_iceberg_format=True)
        assert self._render_statements(relation, changeset) == [
            "alter iceberg table test_db.test_schema.test_model set data_metric_schedule = '5 MINUTE'",
            "alter iceberg table test_db.test_schema.test_model add data metric function snowflake.core.row_count on ()",
        ]

    def test_absent_config_does_not_read_metadata(self):
        relation = _Relation("table")
        _call(
            self.template.module.snowflake__apply_data_metrics,
            relation,
            SimpleNamespace(is_configured=False),
        )
        self.default_context["adapter"].describe_data_metrics.assert_not_called()
        assert self.statements == []

    def test_explicit_empty_list_still_reconciles(self):
        changeset = self._changeset(
            drops=[_association('"SNOWFLAKE"."CORE"."ROW_COUNT"', "")],
        )
        relation = _Relation("table", changeset)
        _call(self.template.module.snowflake__apply_data_metrics, relation, self.data_metrics)
        self.default_context["adapter"].describe_data_metrics.assert_called_once_with(
            relation, self.data_metrics
        )
        assert relation.changeset_calls == [("state", self.data_metrics)]
        assert self.statements == [
            'alter table test_db.test_schema.test_model drop data metric function "SNOWFLAKE"."CORE"."ROW_COUNT" on ()'
        ]

    def test_missing_local_dmf_is_skipped(self):
        # A dbt-built DMF in the model's schema that is not built yet: its ADD is
        # skipped, other changes still run.
        self.default_context["adapter"].missing_local_data_metric_functions.return_value = [
            "COUNT_POSITIVE"
        ]
        changeset = self._changeset(
            adds=[
                _association("db.schema.count_positive", "amount", local_name="COUNT_POSITIVE"),
                _association("snowflake.core.row_count", ""),
            ],
        )
        assert self._render_statements(_Relation("table"), changeset) == [
            "alter table test_db.test_schema.test_model add data metric function snowflake.core.row_count on ()",
        ]

    def test_only_missing_local_dmfs_runs_nothing(self):
        self.default_context["adapter"].missing_local_data_metric_functions.return_value = [
            "COUNT_POSITIVE"
        ]
        changeset = self._changeset(
            adds=[_association("db.schema.count_positive", "amount", local_name="COUNT_POSITIVE")]
        )
        assert self._render_statements(_Relation("table"), changeset) == []

    def test_in_sync_runs_nothing(self):
        relation = _Relation("table", None)
        _call(self.template.module.snowflake__apply_data_metrics, relation, self.data_metrics)
        assert relation.changeset_calls == [("state", self.data_metrics)]
        assert self.statements == []
