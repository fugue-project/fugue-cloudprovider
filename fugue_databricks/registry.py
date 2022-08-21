from typing import Any

from fugue import parse_execution_engine
from triad import ParamDict

from .execution_engine import DatabricksExecutionEngine


def _is_match(engine: Any, conf: Any, **kwargs: Any):
    if not isinstance(engine, str):
        return False
    if engine == "db" or engine.startswith("db:"):
        return True
    if engine == "databricks" or engine.startswith("databricks:"):
        return True
    return False


@parse_execution_engine.candidate(matcher=_is_match, priority=2)
def _parse_db_engine(
    engine: str, conf: Any, **kwargs: Any
) -> DatabricksExecutionEngine:
    e = engine.split(":", 1)
    _conf = ParamDict(conf)
    if len(e) == 2:
        _conf["cluster_id"] = e[1]
    return DatabricksExecutionEngine(conf=_conf)


# TODO: remove after Fugue is fully moved to conditional_dispatcher
def register() -> None:
    pass
