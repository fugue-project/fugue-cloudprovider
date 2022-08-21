import logging
import os
from pathlib import Path
from time import sleep
from typing import Any, Dict, List, Optional, Tuple

from databricks_cli.clusters.api import ClusterApi
from databricks_cli.libraries.api import LibrariesApi
from databricks_cli.sdk.api_client import ApiClient
from pyspark.sql import SparkSession
from requests.exceptions import HTTPError  # type: ignore
from triad import ParamDict, assert_or_throw

from .exceptions import DatabricksCLIError


def get_databricks_version() -> str:
    try:
        from importlib.metadata import version as pkg_version
    except Exception:
        from importlib_metadata import version as pkg_version  # type: ignore
    return pkg_version("databricks-connect")


DATABRICKS_CONNECT_VERSION = get_databricks_version()
_STATE_CHECK_INTERVAL_SEC = 10
_LOG = logging.getLogger(__name__)


class Cluster:
    def __init__(
        self,
        conf: ParamDict,
        api_client: ApiClient,
        ephemeral: bool,
        check_interval_sec: int = _STATE_CHECK_INTERVAL_SEC,
    ):
        self._check_interval_sec = check_interval_sec
        self._manager = ClusterApi(api_client)
        self._libs = LibrariesApi(api_client)
        if "cluster" in conf:  # new cluster
            spec = dict(conf.get_or_throw("cluster", dict))
            self._cluster_id = ParamDict(self._manager.create_cluster(spec))[
                "cluster_id"
            ]
            _LOG.info("Creating a new cluster %s", self._cluster_id)
            if "libraries" in conf:
                ll = conf.get_or_throw("libraries", list)
                self._libs.install_libraries(self._cluster_id, ll)
                _LOG.info("Registered %i libraries", len(ll))
        else:  # existing cluster
            self._cluster_id = conf.get_or_throw("cluster_id", str)
            _LOG.info("Connecting to an existing cluster %s", self._cluster_id)
            self.get_info()
        self._ephemeral = ephemeral
        self._delete_ephemeral = conf.get("delete_ephemeral", False)

    @property
    def cluster_id(self) -> str:
        return self._cluster_id

    @property
    def ephemeral(self) -> bool:
        return self._ephemeral

    def get_info(self) -> Dict[str, Any]:
        return self._manager.get_cluster(self.cluster_id)

    def get_state(self) -> str:
        return self.get_info()["state"]

    def get_libs_info(self) -> List[Any]:
        return self._libs.cluster_status(self.cluster_id).get("library_statuses", [])

    def terminate(self) -> None:
        self._manager.delete_cluster(self.cluster_id)
        _LOG.info("%s is TERMINATED", self.cluster_id)

    def delete(self) -> None:
        self._manager.permanent_delete(self.cluster_id)
        _LOG.info("%s is DELETED", self.cluster_id)

    def stop(self, ignore_error: bool) -> None:
        try:
            if self.ephemeral:
                if self._delete_ephemeral:
                    self.delete()
                else:
                    self.terminate()
        except Exception as e:
            if not ignore_error:
                raise
            _LOG.warning("Failed to stop %s: %s", self.cluster_id, e)

    def blocking_start(self) -> "Cluster":
        try:
            self._blocking_start()
            _LOG.info("%s is READY", self.cluster_id)
            return self
        except Exception:
            self.stop(ignore_error=False)
            raise

    def _blocking_start(self) -> "Cluster":
        state = self.get_state()
        if state == "RUNNING":
            return self._blocking_start_libs()
        if state != "PENDING":
            try:
                self._manager.start_cluster(self.cluster_id)
            except DatabricksCLIError as e:
                if e.code != "INVALID_STATE":
                    raise
        while True:
            sleep(self._check_interval_sec)
            state = self.get_state()
            if state != "PENDING":
                break
            _LOG.info("%s is %s", self.cluster_id, state)
        if state == "RUNNING":
            _LOG.info("%s is %s", self.cluster_id, state)
            return self._blocking_start_libs()
        raise ValueError(f"unexpected state {state} for cluster {self.cluster_id}")

    def _blocking_start_libs(self) -> "Cluster":
        while True:
            info = self.get_libs_info()
            working = any(
                status["status"] in ["PENDING", "RESOLVING", "INSTALLING"]
                for status in info
            )
            if not working:
                _LOG.info("%s installed all libraries", self.cluster_id)
                return self
            _LOG.info("%s is installing libraries", self.cluster_id)
            sleep(self._check_interval_sec)


def get_databricks_spark(conf: Any) -> Tuple[SparkSession, Optional[Cluster]]:
    _conf = ParamDict(conf)
    if len(_conf) == 0:  # currently on databricks notebook
        return _create_session({}), None

    client = _ApiClientWrapper(**_create_api_client_conf(_conf))

    if "cluster_id" in _conf:  # connect to remote existing cluster
        assert_or_throw(
            "cluster" not in _conf and "libraries" not in _conf,
            ValueError(
                "when cluster_id is provided it is to connect to an existing"
                " remote cluster, cluster and libraries can't exist"
            ),
        )
        cluster = Cluster(_conf, client, ephemeral=False)
        return _create_session(_create_session_conf(_conf)), cluster
    # create an on-demand cluster
    assert_or_throw(
        "cluster" in _conf,
        ValueError("cluster is required to create an emphemeral cluster"),
    )
    cluster = Cluster(_conf, client, ephemeral=True)
    _conf["cluster_id"] = cluster.cluster_id
    return _create_session(_create_session_conf(_conf)), cluster


def _create_session_conf(conf: ParamDict) -> Dict[str, Any]:
    res: Dict[str, Any] = {}
    res["spark.databricks.service.clusterId"] = conf.get_or_throw("cluster_id", str)
    if conf.get_or_none("host", str) is not None:
        res["spark.databricks.service.address"] = conf["host"]
    if conf.get_or_none("token", str) is not None:
        res["spark.databricks.service.token"] = conf["token"]
    if conf.get_or_none("org_id", str) is not None:
        res["spark.databricks.service.orgId"] = conf["org_id"]
    if conf.get_or_none("port", int) is not None:
        res["spark.databricks.service.port"] = conf["port"]
    res["spark.ui.showConsoleProgress"] = False
    return res


def _create_api_client_conf(conf: ParamDict) -> Dict[str, Any]:
    """Solving the incompatibility between databricks-connect and databricks-cli"""
    res: Dict[str, Any] = {}
    if conf.get_or_none("host", str) is not None:
        res["host"] = conf["host"]
    elif "DATABRICKS_ADDRESS" in os.environ:
        res["host"] = os.environ["DATABRICKS_ADDRESS"]
    if conf.get_or_none("token", str) is not None:
        res["token"] = conf["token"]
    elif "DATABRICKS_API_TOKEN" in os.environ:
        res["token"] = os.environ["DATABRICKS_API_TOKEN"]
    return res


def _create_session(conf: Dict[str, Any]) -> SparkSession:
    try:
        cfg = Path.home().joinpath(".databricks-connect")
        if not cfg.exists():
            _LOG.warning("databricks-connect was not configured, configuring now")
            cfg.write_text("{}")
    except Exception as e:
        _LOG.warning("Unable to automatically configure databricks-connect %s", e)
    bd = SparkSession.builder
    for k, v in conf.items():
        bd = bd.config(k, v)
    return bd.getOrCreate()


class _ApiClientWrapper(ApiClient):
    def perform_query(
        self,
        method: Any,
        path: Any,
        data: Any = ...,
        headers: Any = None,
        files: Any = None,
        version: Any = None,
    ):
        try:
            return super().perform_query(method, path, data, headers, files, version)
        except HTTPError as e:
            raise DatabricksCLIError(e) from None
