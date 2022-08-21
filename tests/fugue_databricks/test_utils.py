from fugue_databricks._utils import _create_session_conf
from triad import ParamDict


def test_create_session_conf():
    conf = _create_session_conf(ParamDict({"cluster_id": "abc"}))
    assert not conf["spark.ui.showConsoleProgress"]
    assert conf["spark.databricks.service.clusterId"] == "abc"
