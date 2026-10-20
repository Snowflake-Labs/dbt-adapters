# Snowflake data metric functions

DMF support has two lifecycles: creating a custom function and managing its
associations on a model. The model owns its associations and schedule.

## Model bindings

Configure bindings on table, view, incremental, or dynamic table models:

```yaml
models:
  - name: orders
    config:
      data_metric_schedule: "5 MINUTE"
      data_metric_functions:
        - function: SNOWFLAKE.CORE.ROW_COUNT
        - function: SNOWFLAKE.CORE.NULL_COUNT
          columns: [status]
        - function: SNOWFLAKE.CORE.NULL_COUNT
          columns: [amount]
```

Declare all bindings in model-level `config.data_metric_functions`, including
single-column bindings. Column-level declarations are not supported. Each
binding specifies a function and an ordered list of input columns; the list
can contain multiple columns. Omit `columns` or use `columns: []` for functions
such as `ROW_COUNT` that take no input columns.

Omitting `data_metric_functions` leaves associations unmanaged. Setting
`data_metric_functions: []`
removes every existing association, including associations created outside dbt.
Omitting `data_metric_schedule` leaves the schedule unmanaged; an explicit
`null` schedule is rejected.

Bindings are reconciled after the model is built. dbt reads the current state,
adds missing associations, and drops associations outside the desired set.
This restores declared bindings after a table or view is replaced and avoids
binding DDL when an incremental model's state already matches.

Every function name must be fully qualified as
`<database>.<schema>.<function>`, including custom DMFs in the model's schema.
Quoted identifiers are supported. A missing DMF causes the model to fail;
dbt does not skip the binding or create the function implicitly. Snowflake
validates privileges, schedules, function signatures, and column names.

A string in schema YAML or `config()` identifies a Snowflake object. It does
not establish a dbt dependency. Existing system or externally managed DMFs must
already exist when the model runs.

## Project-defined custom DMFs

Create a custom DMF as a SQL function resource. For example,
`functions/count_positive.sql` contains:

```sql
select count(*) from arg_t where arg_col > 0
```

And `functions/count_positive.yml` contains:

```yaml
functions:
  - name: count_positive
    config:
      is_data_metric_function: true
    arguments:
      - name: arg_t
        data_type: TABLE(arg_col NUMBER)
    returns:
      data_type: NUMBER
```

The adapter uses `CREATE OR ALTER DATA METRIC FUNCTION`, which preserves existing
bindings. A DMF returns a scalar value despite accepting a table-shaped argument.
Changing argument types creates another overload; old overloads are not removed.
`volatility` is ignored with a warning.

Each model that uses a project-defined DMF must record the dependency in its SQL:

```sql
-- depends_on: {{ function('count_positive') }}
select 1::number as amount
```

Its binding can still be declared in schema YAML:

```yaml
models:
  - name: orders
    config:
      materialized: table
      data_metric_functions:
        - function: "{{ target.database }}.{{ target.schema }}.count_positive"
          columns: [amount]
```

The example assumes the function uses the target database and schema with no
alias. If its database, schema, or alias differs, configure that actual Snowflake
name in the binding. `function()` in the SQL dependency comment uses the dbt
resource name. The binding string is not automatically resolved through
`function()` and does not follow function deferral.

Use `dbt build --select +orders` to build the model and its function ancestor.
Selecting only the model requires that the DMF already exist. Selecting only
the function updates its definition and preserves its existing bindings; it
does not create bindings on unselected models. A failed function build skips
its dependent models through the existing dbt scheduler.

This implementation requires no dbt-core changes. Current dbt-core versions may
emit `CustomKeyInConfigDeprecation` for the new top-level config keys, including
`is_data_metric_function`; this can fail an invocation using `--warn-error`.
