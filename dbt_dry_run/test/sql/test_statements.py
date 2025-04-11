from dbt_dry_run.results import Results
from dbt_dry_run.sql.statements import create_or_replace_view
from dbt_dry_run.test.utils import SimpleNode


def test_create_or_replace_view_returns_view_ddl() -> None:
    node = SimpleNode(unique_id="a", depends_on=[], language="sql").to_node()
    sql_statement = "SELECT * FROM foo"
    actual = create_or_replace_view(sql_statement, node, Results())
    expected = (
        f"CREATE OR REPLACE VIEW `my_db`.`my_schema`.`a` AS (\n{sql_statement}\n)"
    )
    assert actual == expected
