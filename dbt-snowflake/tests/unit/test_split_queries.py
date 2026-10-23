import random
from io import StringIO
from unittest import mock

import pytest
from snowflake.connector.util_text import split_statements

from dbt.adapters.snowflake.connections import SnowflakeConnectionManager


def reference_split(sql):
    return [part[0] for part in split_statements(StringIO(str(sql)))]


class ReferenceConnectionManager(SnowflakeConnectionManager):
    _split_queries = classmethod(lambda cls, sql: reference_split(sql))


def stripped(manager, sql):
    return manager._stripped_queries(manager, sql)


def assert_equivalent(sql):
    assert SnowflakeConnectionManager._split_queries(sql) == reference_split(sql)
    assert stripped(SnowflakeConnectionManager, sql) == stripped(ReferenceConnectionManager, sql)


CORPUS = [
    "",
    " ",
    "\n",
    " \t\n \n",
    "select 1",
    "\x0c　select 1\x85",
    "  select 1  \n",
    "select 1;",
    "select 1;\n",
    "select 1; select 2",
    "select 1;\nselect 2;\n",
    "select 1;;",
    ";",
    "  ;  ",
    "select 'a;b'",
    "select 'a;b'; select 'c'",
    'select "col;name" from t',
    "select 'it''s' from t",
    "select 'back\\'slash;' from t",
    "select 'unterminated; quote",
    "-- leading comment\nselect 1",
    "select 1 -- trailing; comment",
    "select 1 -- trailing comment\n",
    "-- only a comment",
    "-- only a comment;",
    "/* block */ select 1",
    "/* block; with semicolon */ select 1",
    "/* unterminated block",
    "select 1 /* multi\nline\ncomment */ from t",
    '/* {"app": "dbt"} */\ninsert into t values (1)',
    "create function f() returns int as $$ select 1 $$",
    "create function f() returns int as $$ select 1; select 2; $$;",
    "create procedure p() returns int language sql as $$\nbegin\n  return 1;\nend;\n$$;",
    "begin;",
    "commit;",
    "BEGIN;\nCOMMIT;",
    "begin; insert into t values (1); commit;",
    "!set variable_substitution=true",
    "!set x=1\nselect 1",
    "select 1\n!help",
    "select 1 ! 2",
    "select 1 != 2",
    "select '!'\n",
    "put file:///tmp/a/* @stage",
    "put file:///tmp/a/*; select 1",
    "select 1\r\nselect 2",
    "select 1;\r\nselect 2\r\n",
    "select 1\rselect 2",
    "select $$a;b$$, 'x'",
    "select 1 -- comment\n; select 2",
]


@pytest.mark.parametrize("sql", CORPUS)
def test_split_queries_matches_split_statements(sql):
    assert_equivalent(sql)


def large_insert(semicolons):
    sep = ";" if semicolons else ","
    row = (
        "('id_{i}', 'model.pkg.m_{i}', 'select a{sep} b from \\'x\\' where c = ''d''"
        " -- note{sep}', 1.5, null, '2026-10-20 00:00:00')"
    )
    rows = ", ".join(row.format(i=i, sep=sep) for i in range(5000))
    return (
        '/* {"app": "dbt", "node_id": "operation.elementary"} */\n'
        'insert into "DB"."ELEMENTARY"."DBT_RUN_RESULTS"\n'
        "         (id, unique_id, compiled_code, execution_time, message, generated_at) values\n"
        f"    {rows}\n"
    )


def test_large_single_statement_insert_without_semicolons_skips_split_statements():
    sql = large_insert(semicolons=False)
    expected = reference_split(sql)
    with mock.patch("snowflake.connector.util_text.split_statements") as split:
        assert SnowflakeConnectionManager._split_queries(sql) == expected
        split.assert_not_called()
    assert expected == [sql.strip()]


def test_large_single_statement_insert_with_semicolons_in_literals():
    assert_equivalent(large_insert(semicolons=True))


TOKENS = [
    ";",
    "'",
    '"',
    "\\",
    "''",
    "--",
    "/*",
    "*/",
    "$$",
    "!",
    "\n",
    "\r",
    " ",
    "\t",
    "a",
    "select",
    "begin",
    "commit",
    "file://",
    "PUT",
    ">",
    "$",
    "-",
    "/",
    "*",
]


@pytest.mark.parametrize("with_semicolons", [True, False])
def test_split_queries_matches_split_statements_on_random_input(with_semicolons):
    tokens = TOKENS if with_semicolons else [t for t in TOKENS if t != ";"]
    rng = random.Random(1015)
    for _ in range(5000):
        assert_equivalent("".join(rng.choice(tokens) for _ in range(rng.randint(0, 30))))
