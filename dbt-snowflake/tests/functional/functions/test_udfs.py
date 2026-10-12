import pytest
from dbt.contracts.graph.nodes import FunctionNode
from dbt.contracts.results import RunStatus
from dbt.adapters.events.types import SQLQuery
from dbt.events.types import JinjaLogWarning
from dbt.tests.adapter.functions.files import (
    MY_UDF_WITH_DEFAULT_ARG_YML,
    MY_UDF_YML,
)
from dbt.tests.adapter.functions.test_udfs import (
    UDFsBasic,
    DeterministicUDF,
    StableUDF,
    NonDeterministicUDF,
    ErrorForUnsupportedType,
    PythonUDFSupported,
    PythonUDFRuntimeVersionRequired,
    PythonUDFEntryPointRequired,
    SqlUDFDefaultArgSupport,
    PythonUDFDefaultArgSupport,
    PythonUDFVolatilitySupport,
    CanFindScalarFunctionRelation,
    PythonUDFWithPackagesSupported,
)
from dbt.tests.util import run_dbt
from dbt_common.events.event_catcher import EventCatcher
from tests.functional.functions.files import (
    MY_DMF_COLUMN_MODEL_YML,
    MY_DMF_MODEL_SQL,
    MY_DMF_MODEL_YAML_SQL,
    MY_DMF_MODEL_YML,
    MY_DMF_SQL,
    MY_DMF_YML,
    MY_UDF_SQL,
)


class TestSnowflakeUDFs(UDFsBasic):

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": MY_UDF_SQL,
            "price_for_xlarge.yml": MY_UDF_YML,
        }


class TestSnowflakeDeterministicUDFs(DeterministicUDF):
    pass


class TestSnowflakeStableUDFs(StableUDF):
    def check_function_volatility(self, sql: str):
        assert "VOLATILE" not in sql
        assert "STABLE" not in sql
        assert "IMMUTABLE" not in sql

    def test_udfs(self, project, sql_event_catcher):
        warning_event_catcher = EventCatcher(JinjaLogWarning)
        result = run_dbt(
            ["build", "--debug"], callbacks=[sql_event_catcher.catch, warning_event_catcher.catch]
        )

        assert len(result.results) == 1
        node_result = result.results[0]
        assert node_result.status == RunStatus.Success
        node = node_result.node
        assert isinstance(node, FunctionNode)
        assert node_result.node.name == "price_for_xlarge"

        # Check volatility
        assert len(sql_event_catcher.caught_events) == 1
        self.check_function_volatility(sql_event_catcher.caught_events[0].data.sql)

        # Check that the warning event was caught
        assert len(warning_event_catcher.caught_events) == 1
        assert (
            "Found `stable` volatility specified on function `price_for_xlarge`. This volatility is not supported by snowflake, and will be ignored"
            in warning_event_catcher.caught_events[0].data.msg
        )

        # Check that the function can be executed
        result = run_dbt(["show", "--inline", "SELECT {{ function('price_for_xlarge') }}(100)"])
        assert len(result.results) == 1
        # The result should have an agate table with one row and one column (and thus only one value, which is our inline selection)
        select_value = int(result.results[0].agate_table.rows[0].values()[0])
        assert select_value == 200  # the UDF should return 2x the input value (100 * 2 = 200)


class TestSnowflakeNonDeterministicUDFs(NonDeterministicUDF):
    pass


class TestSnowflakeErrorForUnsupportedType(ErrorForUnsupportedType):
    pass


class TestSnowflakeDataMetricFunction:
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "count_positive.sql": MY_DMF_SQL,
            "count_positive.yml": MY_DMF_YML,
        }

    def test_data_metric_function(self, project):
        sql_event_catcher = EventCatcher(
            event_to_catch=SQLQuery,
            predicate=lambda event: event.data.node_info.node_name == "count_positive"
            and "DATA METRIC FUNCTION" in event.data.sql,
        )
        result = run_dbt(["build", "--debug"], callbacks=[sql_event_catcher.catch])

        assert len(result.results) == 1
        node_result = result.results[0]
        assert node_result.status == RunStatus.Success
        assert isinstance(node_result.node, FunctionNode)

        assert len(sql_event_catcher.caught_events) == 1
        sql = sql_event_catcher.caught_events[0].data.sql
        assert "CREATE OR ALTER DATA METRIC FUNCTION" in sql
        assert "CREATE OR REPLACE" not in sql
        assert "arg_t TABLE(arg_col NUMBER)" in sql
        assert "RETURNS NUMBER" in sql

        # CREATE OR ALTER is idempotent, so a rebuild should also succeed
        run_dbt(["build"])


class TestSnowflakeDataMetricFunctionBinding:
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "count_positive.sql": MY_DMF_SQL,
            "count_positive.yml": MY_DMF_YML,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": MY_DMF_MODEL_SQL,
            "yaml_model.sql": MY_DMF_MODEL_YAML_SQL,
            "yaml_model.yml": MY_DMF_MODEL_YML,
            "column_model.sql": MY_DMF_MODEL_YAML_SQL,
            "column_model.yml": MY_DMF_COLUMN_MODEL_YML,
        }

    def _bound_metrics(self, project, name):
        return project.run_sql(
            "select metric_database_name, metric_schema_name, metric_name from table("
            f"{project.database}.information_schema.data_metric_function_references("
            f"ref_entity_name => '{project.database}.{project.test_schema}.{name}', "
            "ref_entity_domain => 'table'))",
            fetch="all",
        )

    def test_model_binds_dbt_built_dmf(self, project):
        # One fresh build: whichever of the DMF and the models finishes first, the
        # binding ends up in place.
        run_dbt(["build", "--threads", "4"])
        expected = [(project.database.upper(), project.test_schema.upper(), "COUNT_POSITIVE")]
        for name in ("my_model", "yaml_model", "column_model"):
            assert self._bound_metrics(project, name) == expected

    def test_models_built_before_the_dmf(self, project):
        # Force the order: models first while the DMF does not exist, then the DMF.
        project.run_sql(
            f"drop function if exists "
            f"{project.database}.{project.test_schema}.count_positive(TABLE(NUMBER))"
        )
        run_dbt(["build", "--select", "resource_type:model"])
        for name in ("my_model", "yaml_model", "column_model"):
            assert self._bound_metrics(project, name) == []

        run_dbt(["build", "--select", "count_positive"])
        expected = [(project.database.upper(), project.test_schema.upper(), "COUNT_POSITIVE")]
        for name in ("my_model", "yaml_model", "column_model"):
            assert self._bound_metrics(project, name) == expected

        # Rebuilding the function with CREATE OR ALTER keeps the binding.
        run_dbt(["build", "--select", "count_positive"])
        assert self._bound_metrics(project, "my_model") == expected


class TestSnowflakePythonUDFSupported(PythonUDFSupported):
    pass


class TestSnowflakePythonUDFRuntimeVersionRequired(PythonUDFRuntimeVersionRequired):
    pass


class TestSnowflakePythonUDFEntryPointRequired(PythonUDFEntryPointRequired):
    pass


class TestSnowflakeDefaultArgsSupportSQLUDFs(SqlUDFDefaultArgSupport):
    expect_default_arg_support = True

    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": MY_UDF_SQL,
            "price_for_xlarge.yml": MY_UDF_WITH_DEFAULT_ARG_YML,
        }


class TestSnowflakeDefaultArgsSupportPythonUDFs(PythonUDFDefaultArgSupport):
    expect_default_arg_support = True


class TestSnowflakePythonUDFVolatilitySupport(PythonUDFVolatilitySupport):
    pass


class TestSnowflakeCanFindScalarFunctionRelation(CanFindScalarFunctionRelation):
    @pytest.fixture(scope="class")
    def functions(self):
        return {
            "price_for_xlarge.sql": MY_UDF_SQL,
            "price_for_xlarge.yml": MY_UDF_YML,
        }


class TestSnowflakePythonUDFWithPackages(PythonUDFWithPackagesSupported):
    """Python UDF with packages: templating and execution (uses default expected_packages_sql_fragment for Snowflake)."""

    pass
