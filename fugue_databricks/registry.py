from typing import Any

from fugue.plugins import parse_execution_engine
from triad import ParamDict

try:
    from pyspark.sql.connect.session import SparkSession as ConnectSparkSession
except ImportError:
    ConnectSparkSession = None
from .execution_engine import DatabricksExecutionEngine
from fugue_spark import SparkExecutionEngine


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


@parse_execution_engine.candidate(
    lambda engine, conf, **kwargs: ConnectSparkSession is not None
    and isinstance(engine, ConnectSparkSession)
)
def _parse_connect_engine(
    engine: Any, conf: Any, **kwargs: Any
) -> DatabricksExecutionEngine:
    return SparkExecutionEngine(engine, conf=conf)
