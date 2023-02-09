from concurrent import futures
from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional, Tuple

from dbt_dry_run.adapter.service import ProjectService
from dbt_dry_run.linting.column_linting import lint_columns
from dbt_dry_run.sql_runner import SQLRunner

if TYPE_CHECKING:
    from mypy.typeshed.stdlib.concurrent.futures._base import Future
else:
    from typing import Awaitable as Future

from dbt_dry_run.exception import ManifestValidationError, NodeExecutionException
from dbt_dry_run.models.manifest import Manifest, Node
from dbt_dry_run.node_runner import NodeRunner, get_runner_map
from dbt_dry_run.node_runner.model_runner import ModelRunner
from dbt_dry_run.node_runner.node_test_runner import NodeTestRunner
from dbt_dry_run.node_runner.seed_runner import SeedRunner
from dbt_dry_run.node_runner.snapshot_runner import SnapshotRunner
from dbt_dry_run.node_runner.source_runner import SourceRunner
from dbt_dry_run.results import DryRunResult, Results
from dbt_dry_run.scheduler import ManifestScheduler
from dbt_dry_run.sql_runner.big_query_sql_runner import BigQuerySQLRunner

CONCURRENCY = 8

_RUNNER_CLASSES: List[Any] = [
    ModelRunner,
    SeedRunner,
    SnapshotRunner,
    NodeTestRunner,
    SourceRunner,
]

_RUNNERS = get_runner_map(_RUNNER_CLASSES)


def dispatch_node(node: Node, runners: Dict[str, NodeRunner]) -> DryRunResult:
    try:
        runner = runners[node.resource_type]
    except KeyError:
        raise ValueError(f"Unknown resource type '{node.resource_type}'")
    validation_result = runner.validate_node(node)
    if validation_result:
        return validation_result
    return runner.run(node)


def dry_run_node(runners: Dict[str, NodeRunner], node: Node, results: Results) -> None:
    """
    This method must be thread safe
    """
    dry_run_result = dispatch_node(node, runners)
    if node.get_should_check_columns():
        dry_run_result = lint_columns(node, dry_run_result)
    results.add_result(node.unique_id, dry_run_result)


@contextmanager
def create_context(
    project: ProjectService,
) -> Generator[Tuple[SQLRunner, ThreadPoolExecutor], None, None]:
    sql_runner: Optional[SQLRunner] = None
    executor: Optional[ThreadPoolExecutor] = None
    try:
        sql_runner = BigQuerySQLRunner(project)
        executor = ThreadPoolExecutor(max_workers=project.threads)
        yield sql_runner, executor
    finally:
        if executor:
            executor.shutdown()


def validate_manifest_compatibility(manifest: Manifest) -> None:
    failing_nodes: List[Tuple[str, str]] = []
    for key, node in manifest.nodes.items():
        if node.language is not None and node.language != "sql":
            failing_nodes.append(
                (
                    "NODE_NOT_SQL : Only SQL language models are supported",
                    node.unique_id,
                )
            )

    if failing_nodes:
        formatted_errors = "\n".join(
            map(lambda failure: f"{failure[0]} : {failure[1]}", failing_nodes)
        )
        raise ManifestValidationError(
            f"Manifest nodes failed validation due to:\n{formatted_errors}"
        )


def dry_run_manifest(project: ProjectService) -> Results:
    executor: ThreadPoolExecutor
    with create_context(project) as (sql_runner, executor):
        results = Results()
        runners = {t: runner(sql_runner, results) for t, runner in _RUNNERS.items()}
        manifest = project.get_dbt_manifest()

        validate_manifest_compatibility(manifest)

        scheduler = ManifestScheduler(manifest)

        print(f"Dry running {len(scheduler)} nodes")
        for generation_id, generation in enumerate(scheduler):
            gen_futures: Dict[str, Future[None]] = {}
            for node in generation:
                task_future: Future[None] = executor.submit(
                    dry_run_node, runners, node, results
                )
                gen_futures[node.unique_id] = task_future
            _wait_for_generation(gen_futures)

        results.finish()
    return results


def _wait_for_generation(node_futures: Dict[str, Future[None]]) -> None:
    futures.wait(list(node_futures.values()), timeout=None)
    for node_id, fut in node_futures.items():
        try:
            fut.result()
        except Exception as e:
            msg = f"Node {node_id} raised unhandled exception '{e.__class__.__name__}'"
            raise NodeExecutionException(msg) from e
