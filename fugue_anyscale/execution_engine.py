from fugue import DataFrame
from fugue_ray import RayExecutionEngine

from .cluster import Cluster


class AnyscaleExecutionEngine(RayExecutionEngine):
    def __init__(self, cluster: Cluster, stop_cluster: bool = True):
        self._cluster = cluster
        self._stop_cluster = stop_cluster
        super().__init__(conf=cluster.conf)

    def stop_engine(self) -> None:
        if self._stop_cluster:
            self._cluster.stop(ignore_error=False)

    def convert_yield_dataframe(self, df: DataFrame, as_local: bool) -> DataFrame:
        if self._stop_cluster and self._ctx_count <= 1:
            return df.as_local()
        return super().convert_yield_dataframe(df, as_local)
