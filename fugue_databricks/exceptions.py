import requests  # type: ignore
from fugue.exceptions import FugueError


class DatabricksCLIError(FugueError):
    def __init__(self, error: requests.exceptions.HTTPError):
        p = error.response.json()
        self._code = p["error_code"]
        super().__init__("(" + p["error_code"] + ") " + p["message"])

    @property
    def code(self) -> str:
        return self._code
