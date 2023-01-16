from typing import Any

from fugue import ExecutionEngine
from fugue.plugins import parse_execution_engine
from triad import ParamDict, assert_or_throw

from .cluster import Cluster
from .execution_engine import AnyscaleExecutionEngine


# TODO: remove after Fugue is fully moved to conditional_dispatcher
def register() -> None:
    pass


@parse_execution_engine.candidate(
    matcher=lambda engine, conf, **kwargs: isinstance(engine, str)
    and engine.startswith("anyscale"),
    priority=2.5,
)
def _parse_anyscale(engine: str, conf: Any, **kwargs) -> ExecutionEngine:
    conf = ParamDict(conf)
    if engine.startswith("anyscale://"):
        conf["address"] = engine
    return AnyscaleExecutionEngine(Cluster(conf), stop_cluster=True)


@parse_execution_engine.candidate(
    matcher=lambda engine, conf, **kwargs: isinstance(engine, Cluster),
    priority=2.5,
)
def _parse_anyscale_cluster(engine: Cluster, conf: Any, **kwargs) -> ExecutionEngine:
    assert_or_throw(
        conf is None, ValueError("can't specify conf when the engine is a Cluster")
    )
    return AnyscaleExecutionEngine(engine, stop_cluster=False)
