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
    columns:
      - name: amount
        data_metric_functions:
          - SNOWFLAKE.CORE.NULL_COUNT
```

Model-level and column-level declarations form one desired set. Omitting all
`data_metric_functions` declarations leaves associations unmanaged. Setting
`data_metric_functions: []` on the model, with no column-level declarations,
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
