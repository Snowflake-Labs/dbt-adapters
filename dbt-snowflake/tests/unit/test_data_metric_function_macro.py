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


class _Config(dict):
    def get(self, key, default=None):
        return super().get(key, default)


class TestSnowflakeDataMetricFunctionMacro(unittest.TestCase):
    def setUp(self):
        self.warnings = []
        self.model = SimpleNamespace(
            name="count_positive",
            config=_Config(is_data_metric_function=True),
            returns=SimpleNamespace(data_type="NUMBER"),
        )
        context = {
            "model": self.model,
            "return": _raise_return,
            "exceptions": SimpleNamespace(warn=self.warnings.append),
            "formatted_scalar_function_args_sql": lambda: "arg_t TABLE(arg_col NUMBER)",
            "scalar_function_body_sql": lambda: "$$ SELECT COUNT(*) FROM arg_t $$",
        }
        env = Environment(loader=FileSystemLoader(MACROS_DIR), extensions=["jinja2.ext.do"])
        self.template = env.get_template(
            "materializations/functions/data_metric.sql", globals=context
        )
        self.relation = mock.Mock()
        self.relation.render.return_value = "db.schema.count_positive"

    def _is_dmf(self):
        return _call(self.template.module.snowflake__is_data_metric_function)

    def _render_dmf(self):
        sql = self.template.module.snowflake__data_metric_function_sql(self.relation)
        return re.sub(r"\s+", " ", sql).strip()

    def test_marker(self):
        assert self._is_dmf() is True
        # Absent and `false` both mean a regular scalar function.
        for config in (_Config(), _Config(is_data_metric_function=False)):
            self.model.config = config
            assert self._is_dmf() is False
        # Only a real `true` counts, not a truthy string.
        self.model.config = _Config(is_data_metric_function="yes")
        assert self._is_dmf() is False

    def test_renders_create_or_alter(self):
        assert self._render_dmf() == (
            "CREATE OR ALTER DATA METRIC FUNCTION db.schema.count_positive "
            "(arg_t TABLE(arg_col NUMBER)) RETURNS NUMBER AS $$ SELECT COUNT(*) FROM arg_t $$"
        )
        assert self.warnings == []

    def test_volatility_is_ignored_with_warning(self):
        self.model.config["volatility"] = "deterministic"
        assert "IMMUTABLE" not in self._render_dmf()
        assert len(self.warnings) == 1
        assert "volatility" in self.warnings[0]
