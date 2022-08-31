from typing import Any

from fugue import DataFrame
from fugue_ray import RayExecutionEngine

from .cluster import Cluster


class AnyscaleExecutionEngine(RayExecutionEngine):
    def __init__(self, cluster: Cluster, conf: Any = None):
        self._cluster = cluster
        super().__init__(conf=conf)

    def stop_engine(self) -> None:
        self._cluster.stop(ignore_error=False)

    def convert_yield_dataframe(self, df: DataFrame, as_local: bool) -> DataFrame:
        if not self._cluster.ephemeral:
            return super().convert_yield_dataframe(df, as_local)
        return df.as_local()
