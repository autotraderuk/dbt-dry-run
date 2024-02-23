from unittest.mock import MagicMock

from dbt_dry_run.models.manifest import ExternalConfig, ManifestColumn, Node, NodeConfig
from dbt_dry_run.node_runner.source_runner import SourceRunner
from dbt_dry_run.results import DryRunStatus, Results


def test_external_source_with_columns_but_no_dry_run_columns() -> None:
    # Create a Node with an external source that has columns but no dry_run_columns
    node = Node(
        unique_id="S",
        resource_type="source",
        config=NodeConfig(),
        name="s",
        database="db1",
        schema="schema1",
        original_file_path="/filepath1.yaml",
        root_path="/filepath1",
        columns={
            "column1": ManifestColumn(name="column1", data_type="STRING"),
            "column2": ManifestColumn(name="column2", data_type="RECORD[]"),
        },
        alias="s",
        external=ExternalConfig(location="location"),  # No dry_run_columns specified
    )

    mock_sql_runner = MagicMock()
    mock_results = MagicMock()

    source_runner = SourceRunner(mock_sql_runner, mock_results)
    result = source_runner.run(node)

    # The test should pass if no InvalidColumnSpecification exception is raised
    assert result.status != DryRunStatus.FAILURE
