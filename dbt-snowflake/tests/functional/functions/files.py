from dbt.tests.adapter.functions.files import MASK_PII_JS  # noqa: F401
from dbt.tests.adapter.functions.files import MY_JS_UDF  # noqa: F401
from dbt.tests.adapter.functions.files import SUM_POSITIVE_JS  # noqa: F401

MY_UDF_SQL = """
price * 2
""".strip()


# --- JavaScript UDF YAML configs (Snowflake-specific types) ---

MY_JS_UDF_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    arguments:
      - name: price
        data_type: float
    returns:
      data_type: float
"""

MY_JS_UDF_WITH_DEFAULT_ARG_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    arguments:
      - name: price
        data_type: float
        default_value: 100
    returns:
      data_type: float
"""

MY_JS_UDF_SECURE_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    config:
      snowflake:
        secure: true
    arguments:
      - name: price
        data_type: float
    returns:
      data_type: float
"""

MY_JS_UDF_NULL_HANDLING_STRICT_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    config:
      snowflake:
        null_handling: strict
    arguments:
      - name: price
        data_type: float
    returns:
      data_type: float
"""

MY_JS_UDF_NULL_HANDLING_CALLED_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    config:
      snowflake:
        null_handling: called
    arguments:
      - name: price
        data_type: float
    returns:
      data_type: float
"""

MY_JS_UDF_LOG_LEVEL_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    config:
      snowflake:
        log_level: info
    arguments:
      - name: price
        data_type: float
    returns:
      data_type: float
"""

MY_JS_UDF_TRACE_LEVEL_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    config:
      snowflake:
        trace_level: "off"
    arguments:
      - name: price
        data_type: float
    returns:
      data_type: float
"""

MY_JS_UDF_ALL_CONFIGS_YML = """
functions:
  - name: price_for_xlarge
    description: Calculate the price for the xlarge version of a standard item
    config:
      volatility: deterministic
      snowflake:
        secure: true
        null_handling: strict
        log_level: info
        trace_level: "off"
    arguments:
      - name: price
        data_type: float
        default_value: 100
    returns:
      data_type: float
"""

MY_JS_UDF_QUOTE_ARGS_FALSE_MULTI_ARG_YML = """
functions:
  - name: compute_total
    description: Multiplies price by quantity
    config:
      snowflake:
        quote_args: false
    arguments:
      - name: price
        data_type: float
      - name: quantity
        data_type: float
    returns:
      data_type: float
"""

MY_JS_UDF_QUOTE_ARGS_TRUE_MULTI_ARG_YML = """
functions:
  - name: compute_total
    description: Multiplies price by quantity
    config:
      snowflake:
        quote_args: true
    arguments:
      - name: price
        data_type: float
      - name: quantity
        data_type: float
    returns:
      data_type: float
"""

COMPUTE_TOTAL_JS_LOWERCASE = """
return price * quantity;
""".strip()

COMPUTE_TOTAL_JS_UPPERCASE = """
return PRICE * QUANTITY;
""".strip()

MASK_PII_JS_YML = """
functions:
  - name: mask_pii
    description: Masks all but the first 2 characters of a string
    arguments:
      - name: value
        data_type: varchar
    returns:
      data_type: varchar
"""

# JS aggregate UDF — not supported on Snowflake, should error
SUM_POSITIVE_JS_YML = """
functions:
  - name: sum_positive
    config:
      type: aggregate
    arguments:
      - name: x
        data_type: float
    returns:
      data_type: float
"""


# --- Data metric function (scalar function marked with is_data_metric_function) ---

MY_DMF_SQL = """
SELECT COUNT(*) FROM arg_t WHERE arg_col > 0
""".strip()

MY_DMF_YML = """
functions:
  - name: count_positive
    description: Count rows with a positive value
    config:
      is_data_metric_function: true
    arguments:
      - name: arg_t
        data_type: TABLE(arg_col NUMBER)
    returns:
      data_type: NUMBER
"""

# A table model that attaches the dbt-built DMF above by its one-part name; it is
# built in the model's schema, so the name resolves there.
MY_DMF_MODEL_SQL = """
{{ config(
    materialized='table',
    data_metric_functions=[{'function': 'count_positive', 'columns': ['amount']}],
) }}
select 1::number as amount union all select -1::number
""".strip()

MY_DMF_MODEL_YAML_SQL = "select 1::number as amount union all select -1::number"

MY_DMF_MODEL_YML = """
models:
  - name: yaml_model
    config:
      materialized: table
      data_metric_functions:
        - function: count_positive
          columns: [amount]
"""

MY_DMF_COLUMN_MODEL_YML = """
models:
  - name: column_model
    config:
      materialized: incremental
    columns:
      - name: amount
        data_metric_functions: [count_positive]
"""
