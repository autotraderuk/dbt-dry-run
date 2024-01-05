from concurrent import futures
from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import contextmanager
from typing import TYPE_CHECKING, Dict, Generator, List, Optional, Tuple

from dbt_dry_run import flags
from dbt_dry_run.adapter.service import ProjectService
from dbt_dry_run.linting.column_linting import lint_columns
from dbt_dry_run.node_dispatch import RUNNERS, RunnerKey, dispatch_node
from dbt_dry_run.sql_runner import SQLRunner

if TYPE_CHECKING:
    from mypy.typeshed.stdlib.concurrent.futures._base import Future
else:
    from typing import Awaitable as Future

from dbt_dry_run.exception import ManifestValidationError, NodeExecutionException
from dbt_dry_run.models.manifest import Manifest, Node
from dbt_dry_run.node_runner import NodeRunner
from dbt_dry_run.results import Results
from dbt_dry_run.scheduler import ManifestScheduler
from dbt_dry_run.sql_runner.big_query_sql_runner import BigQuerySQLRunner


def should_check_columns(node: Node) -> bool:
    check_column = node.get_combined_metadata("dry_run.check_columns")

    if check_column is not None:
        return bool(check_column)

    if flags.EXTRA_CHECK_COLUMNS_METADATA_KEY is not None:
        extra_check_column = node.get_combined_metadata(
            flags.EXTRA_CHECK_COLUMNS_METADATA_KEY
        )
        return bool(extra_check_column) if extra_check_column is not None else False

    return False


def dry_run_node(
    runners: Dict[RunnerKey, NodeRunner], node: Node, results: Results
) -> None:
    """
    This method must be thread safe
    """
    dry_run_result = dispatch_node(node, runners)
    if should_check_columns(node):
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
        runners = {t: runner(sql_runner, results) for t, runner in RUNNERS.items()}
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
