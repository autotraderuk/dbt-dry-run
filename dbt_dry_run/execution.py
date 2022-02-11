from concurrent import futures
from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, Generator, List, Optional, Tuple

from google.cloud.bigquery import Client

from dbt_dry_run.sql_runner import SQLRunner

if TYPE_CHECKING:
    from mypy.typeshed.stdlib.concurrent.futures._base import Future
else:
    from typing import Awaitable as Future

from dbt_dry_run.exception import NodeExecutionException, NotCompiledException
from dbt_dry_run.manifest import Manifest, Node
from dbt_dry_run.models import DryRunResult, DryRunStatus, Output
from dbt_dry_run.node_runner import NodeRunner
from dbt_dry_run.node_runner.model_runner import ModelRunner
from dbt_dry_run.node_runner.seed_runner import SeedRunner
from dbt_dry_run.results import Results
from dbt_dry_run.scheduler import ManifestScheduler
from dbt_dry_run.sql_runner.big_query_sql_runner import BigQuerySQLRunner

CONCURRENCY = 8

_RUNNER_CLASSES: List[Any] = [ModelRunner, SeedRunner]
_RUNNERS = {runner.resource_type: runner for runner in _RUNNER_CLASSES}


def dispatch_node(node: Node, runners: Dict[str, NodeRunner]) -> DryRunResult:
    try:
        return runners[node.resource_type].run(node)
    except KeyError:
        raise ValueError(f"Unknown resource type '{node.resource_type}'")


def dry_run_node(runners: Dict[str, NodeRunner], node: Node, results: Results) -> None:
    """
    This method must be thread safe
    """
    if node.compiled:
        dry_run_result = dispatch_node(node, runners)
        results.add_result(node.unique_id, dry_run_result)
    else:
        not_compiled_result = DryRunResult(
            node=node,
            table=None,
            status=DryRunStatus.FAILURE,
            exception=NotCompiledException(f"Node {node.unique_id} was not compiled"),
        )
        results.add_result(node.unique_id, not_compiled_result)


@contextmanager
def create_context(
    output: Output,
) -> Generator[Tuple[SQLRunner, ThreadPoolExecutor], None, None]:
    sql_runner: Optional[SQLRunner] = None
    executor: Optional[ThreadPoolExecutor] = None
    try:
        sql_runner = BigQuerySQLRunner.from_profile(output)
        executor = ThreadPoolExecutor(max_workers=CONCURRENCY)
        yield sql_runner, executor
    finally:
        if executor:
            executor.shutdown()
        if sql_runner:
            sql_runner.close()


def dry_run_manifest(
    manifest: Manifest, output: Output, model: Optional[str]
) -> Results:
    client: Client
    executor: ThreadPoolExecutor
    with create_context(output) as (sql_runner, executor):
        results = Results()
        runners = {t: runner(sql_runner, results) for t, runner in _RUNNERS.items()}
        scheduler = ManifestScheduler(manifest, model)

        print(f"Dry running {len(scheduler)} models")
        for generation_id, generation in enumerate(scheduler):
            gen_futures: Dict[str, Future[None]] = {}
            for node in generation:
                task_future: Future[None] = executor.submit(
                    dry_run_node, runners, node, results
                )
                gen_futures[node.unique_id] = task_future
            _wait_for_generation(gen_futures)

    return results


def _wait_for_generation(node_futures: Dict[str, Future[None]]) -> None:
    futures.wait(list(node_futures.values()), timeout=None)
    for node_id, fut in node_futures.items():
        try:
            fut.result()
        except Exception as e:
            msg = f"Node {node_id} raised unhandled exception '{e.__class__.__name__}'"
            raise NodeExecutionException(msg) from e
