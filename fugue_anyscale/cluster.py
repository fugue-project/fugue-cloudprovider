import logging
import os
from typing import Any, Iterable

import ray
from anyscale import AnyscaleSDK
from fugue_ray._constants import FUGUE_RAY_CONF_SHUFFLE_PARTITIONS
from triad import ParamDict, assert_or_throw

_LOG = logging.getLogger(__name__)


class Cluster:
    def __init__(
        self,
        conf: Any,
    ):
        self._conf = ParamDict(conf)
        self._sdk = AnyscaleSDK(self._conf.get_or_none("token", str))
        if "token" in self._conf:
            os.environ["ANYSCALE_CLI_TOKEN"] = self._conf.get_or_none("token", str)
        self._ephemeral = self._conf.get("ephemeral", False)
        if "address" in self._conf:
            assert_or_throw(
                "cluster" not in self._conf,
                ValueError(
                    "one and only one of address and cluster"
                    " can exist in Anyscale config"
                ),
            )
            self._ctx = ray.init(self._conf.get_or_throw("address", str))
            self._cluster = self._sdk.get_cluster(
                self._ctx.anyscale_cluster_info.cluster_id
            ).result
        elif "cluster" in self._conf:  # new cluster
            _LOG.info("Launching cluster ...")
            self._cluster = self._sdk.launch_cluster(
                **self._conf.get_or_throw("cluster", dict)
            )
            prj = self._sdk.get_project(self._cluster.project_id).result
            addr = "anyscale://" + prj.name + "/" + self._cluster.name
            _LOG.info("%s has launched", addr)
            self._ctx = ray.init(addr)
        else:  # existing cluster
            raise ValueError(
                "one and only one of address and cluster"
                " can exist in Anyscale config"
            )

        if FUGUE_RAY_CONF_SHUFFLE_PARTITIONS not in self._conf:
            cpus = self._get_all_cpus()
            if cpus > 0:
                self._conf[FUGUE_RAY_CONF_SHUFFLE_PARTITIONS] = cpus * 2

    @property
    def conf(self) -> ParamDict:
        return self._conf

    @property
    def cluster_id(self) -> str:
        return self._cluster.id

    @property
    def ephemeral(self) -> bool:
        return self._ephemeral

    def stop(self, ignore_error: bool) -> None:
        try:
            ray.shutdown()
            if self.ephemeral:
                self._sdk.terminate_cluster(self.cluster_id, {})
            _LOG.info("%s is being terminated", self.cluster_id)
        except Exception as e:
            if not ignore_error:
                raise
            _LOG.warning("Failed to stop %s: %s", self.cluster_id, e)

    def __enter__(self) -> Any:
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, exc_traceback: Any) -> None:
        self.stop(ignore_error=True)

    def _get_all_cpus(self):
        config = self._sdk.get_cluster_compute(
            self._cluster.cluster_compute_id
        ).result.config

        def _get_cpus(node_type: Any, is_head: bool) -> int:
            from fugue_aws._instances_data import AWS_INSTANCE_TYPES

            res = AWS_INSTANCE_TYPES[
                AWS_INSTANCE_TYPES.instance_type == node_type.instance_type
            ]
            try:
                cpus = int(res.vcpus.iloc[0])
                return cpus if is_head else cpus * node_type.max_workers
            except Exception:
                return 0

        def _get() -> Iterable[int]:
            yield _get_cpus(config.head_node_type, True)
            for nt in config.worker_node_types:
                yield _get_cpus(nt, False)

        cpus = 0
        for x in _get():
            if x == 0:
                return 0
            cpus += x
        return cpus
