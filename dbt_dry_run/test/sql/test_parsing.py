import pytest

from dbt_dry_run.sql.parsing import sql_has_recursive_ctes


@pytest.mark.parametrize(
    "code, has_ctes",
    [
        (
            """WITH RECURSIVE my_foo as (SELECT * FROM foo)""",
            True,
        ),
        (
            """with recursive my_foo as (SELECT * FROM foo)""",
            True,
        ),
        (
            """  with   recursive    my_foo as (SELECT * FROM foo)""",
            True,
        ),
        (
            """  with
                  RECURSIVE 
                     my_foo as (SELECT * FROM foo)""",
            True,
        ),
        (
            """  with
                     my_foo as (SELECT recursive FROM foo)""",
            False,
        ),
    ],
)
def test_sql_has_recursive_ctes(code: str, has_ctes: bool) -> None:
    assert sql_has_recursive_ctes(code) == has_ctes
