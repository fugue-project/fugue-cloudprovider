from contextlib import contextmanager
from typing import Any, Iterator

from fugue import DataFrame
from fugue_spark import SparkExecutionEngine
from pyspark.sql import SparkSession

from ._utils import get_databricks_spark


def init_db_spark(conf: Any = None) -> SparkSession:
    session, cluster = get_databricks_spark(conf)
    if cluster is not None:
        cluster.blocking_start()
    return session


@contextmanager
def db_spark(conf: Any = None) -> Iterator[SparkSession]:
    session, cluster = get_databricks_spark(conf)
    try:
        if cluster is not None:
            cluster.blocking_start()
        yield session
    finally:
        if cluster is not None:
            cluster.stop(ignore_error=True)


class DatabricksExecutionEngine(SparkExecutionEngine):
    def __init__(self, conf: Any = None):
        session, self._cluster = get_databricks_spark(conf)
        if self._cluster is not None:
            try:
                self._cluster.blocking_start()
            except Exception:
                self._cluster.stop(ignore_error=False)
                raise
        super().__init__(spark_session=session, conf=conf)

    @property
    def ephemeral(self) -> bool:
        return self._cluster is not None and self._cluster.ephemeral

    def stop_engine(self) -> None:
        if self._cluster is not None:
            self._cluster.stop(ignore_error=True)

    def convert_yield_dataframe(self, df: DataFrame, as_local: bool) -> DataFrame:
        if not self.ephemeral or self._ctx_count > 1:
            return super().convert_yield_dataframe(df, as_local)
        return df.as_local()
