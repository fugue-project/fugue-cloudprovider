import os
from typing import Any, Callable

import dask
from coiled import Cluster
from dask.distributed import Client, get_client
from fugue import ExecutionEngine, make_execution_engine, parse_execution_engine
from triad import ParamDict, assert_or_throw

from .client import CoiledDaskClient


# TODO: remove after Fugue is fully moved to conditional_dispatcher
def register() -> None:
    pass


@parse_execution_engine.candidate(
    matcher=lambda engine, conf, **kwargs: isinstance(engine, str)
    and (engine == "coiled" or engine.startswith("coiled:"))
)
def _parse_coiled(engine: str, conf: Any, **kwargs) -> ExecutionEngine:
    p = engine.split(":", 1)
    conf = ParamDict(conf)
    if "token" in conf:
        dask.config.set({"coiled.token": conf.get_or_throw("token", str)})
        os.environ["DASK_COILED__TOKEN"] = conf.get_or_throw("token", str)
    if len(p) == 1:
        if "cluster" not in conf:
            client = get_client()
            assert_or_throw(
                isinstance(client.cluster, Cluster),
                ValueError("the current Dask client is not a Coiled client"),
            )
            return make_execution_engine(client, conf=conf)
        else:
            # build an ephemeral cluster, will be stopped when the engine is stopped
            def new_stop(old_stop: Callable, dask_client: Client):
                try:
                    old_stop()
                finally:
                    try:
                        dask_client.close()
                    except Exception:
                        pass

            client = CoiledDaskClient(**conf.get_or_throw("cluster", dict))
            res = make_execution_engine(client, conf)
            # TODO: this is a hack, fix it!
            res._orig_stop = res.stop_engine
            res.stop_engine = lambda: new_stop(res._orig_stop, client)
            return res

    else:  # coiled:<clustername>
        cluster = Cluster(name=p[1])
        return make_execution_engine(cluster, conf=conf)


@parse_execution_engine.candidate(
    matcher=lambda engine, conf, **kwargs: isinstance(engine, Cluster)
)
def _parse_coiled_cluster(engine: Cluster, conf: Any, **kwargs) -> ExecutionEngine:
    return make_execution_engine(Client(engine), conf=conf)
