from typing import Any

from fugue import ExecutionEngine, parse_execution_engine
from triad import ParamDict

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
def _parse_coiled(engine: str, conf: Any, **kwargs) -> ExecutionEngine:
    conf = ParamDict(conf)
    if engine.startswith("anyscale://"):
        conf["address"] = engine
    return AnyscaleExecutionEngine(Cluster(conf), conf=conf)
