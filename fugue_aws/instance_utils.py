from math import ceil, floor
from typing import Any, Dict, Optional

import pandas as pd

from ._instances_data import AWS_INSTANCE_TYPES


class EC2InstanceTypes:
    def __init__(self, df: Optional[pd.DataFrame] = None):
        self._df = df if df is not None else AWS_INSTANCE_TYPES

    def filter(self, **kwargs: Any) -> "EC2InstanceTypes":  # noqa
        tdf = self._df
        for k, v in kwargs.items():
            if tdf.shape[0] == 0:
                break
            if isinstance(v, (int, float, str, bool)):
                tdf = tdf[tdf[k] == v]
            elif callable(v):
                tdf = tdf[tdf[k].apply(v)]
        return EC2InstanceTypes(tdf)

    def match_worker(
        self,
        cores: int = 1,
        memory_gb: float = 0.0,
        storage_gb: float = 0.0,
        gpus: int = 0,
        zone: Optional[str] = None,
        **kwargs: Any
    ) -> "EC2InstanceTypes":
        ins = self.filter(**kwargs)
        if zone is not None:
            ins = ins.filter(availability_zones=lambda x: zone in x)
        return ins.filter(
            cores=lambda x: cores <= x,
            memory_gb=lambda x: memory_gb <= x,
            storage_gb=lambda x: storage_gb <= x,
            gpus=lambda x: gpus <= x,
        )

    def suggest_worker_instance(
        self,
        num_workers: int,
        cores: int = 1,
        memory_gb: float = 0.0,
        storage_gb: float = 0.0,
        gpus: int = 0,
        zone: Optional[str] = None,
        **kwargs: Any
    ) -> Dict[str, Any]:
        def compute_worker_n(requested: Any, provided: Any) -> int:
            if float(requested) <= 0:
                return num_workers
            return floor(float(provided) / float(requested))

        def compute_instances(row: Any) -> int:
            n1 = compute_worker_n(cores, row["cores"])
            n2 = compute_worker_n(memory_gb, row["memory_gb"])
            n3 = compute_worker_n(storage_gb, row["storage_gb"])
            n4 = compute_worker_n(gpus, row["gpus"])
            return ceil(num_workers / min([n1, n2, n3, n4]))

        candidates = self.match_worker(
            cores=cores,
            memory_gb=memory_gb,
            storage_gb=storage_gb,
            gpus=gpus,
            zone=zone,
            **kwargs
        )._df
        if candidates.shape[0] == 0:
            raise ValueError("Unable to match with an EC2 instance")
        num_instances = candidates.apply(compute_instances, axis=1)
        cost = num_instances * candidates.on_demand_linux_pricing
        candidates = candidates.assign(
            num_workers=num_workers, num_instances=num_instances, cost=cost
        ).sort_values(by=["cost", "num_instances"])
        return candidates
