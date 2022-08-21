from typing import Any

import coiled
from dask.distributed import Client


class CoiledDaskClient(Client):
    def __init__(self, **kwargs):
        cluster = coiled.Cluster(**kwargs)
        super().__init__(cluster)

    def close(self) -> None:
        super().close()
        self.cluster.close()

    def __enter__(self) -> Any:
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback) -> None:
        self.close()
