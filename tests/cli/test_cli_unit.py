import json
import pytest
from unittest.mock import Mock, patch

from cli.main import (
    GlobalOptions,
    ParseGlobalOptions,
    BuildBaseUrl,
    HandleTasksList,
    HandleTasksSubmit,
    HandleAgentsList,
    HandleErrorResult,
    PrintKeyValueTable,
    StartsWith,
    ParseInt,
    IsValidTaskId,
    EnsureScheme,
    HttpResult,
    ApiClientInterface
)

# ----------------------------------------------------------------------
# Fake API client for testing
# ----------------------------------------------------------------------
class FakeApiClient(ApiClientInterface):
    def __init__(self):
        self.requests = []
        self.get_results = []
        self.post_results = []
        self.default_result = HttpResult(status=200)

    def get(self, path, params=None):
        self.requests.append(("GET", path, params))
        if self.get_results:
            return self.get_results.pop(0)
        return self.default_result

    def post(self, path, body, content_type):
        self.requests.append(("POST", path, body, content_type))
        if self.post_results:
            return self.post_results.pop(0)
        return self.default_result

# ----------------------------------------------------------------------
# Tests
# ----------------------------------------------------------------------
def test_parse_global_options():
    args = [
        "--base-url", "example.com", "--host", "10.0.0.2", "--port", "9090",
        "--timeout-ms=4000", "--json=true", "--verbose=false",
        "tasks", "list"
    ]
    opts = GlobalOptions()
    rest = []
    error = ""
    ok = ParseGlobalOptions(args, opts, rest, error)
    assert ok
    assert opts.base_url == "example.com"
    assert opts.host == "10.0.0.2"
    assert opts.port == 9090
    assert opts.timeout_ms == 4000
    assert opts.json_output is True
    assert opts.verbose is False
    assert rest == ["tasks", "list"]

def test_parse_global_options_unknown():
    opts = GlobalOptions()
    rest = []
    error = ""
    ok = ParseGlobalOptions(["--bad-flag"], opts, rest, error)
    assert not ok
    assert "Unknown option" in error

def test_build_base_url():
    opts = GlobalOptions()
    opts.base_url = "example.com"
    opts.host = "ignored"
    opts.port = 9999
    assert BuildBaseUrl(opts) == "http://example.com"

def test_handle_tasks_list():
    client = FakeApiClient()
    client.get_results.append(HttpResult(status=200, body='{"tasks":[{"task_id":"t1","state":"queued"}]}'))
    opts = GlobalOptions()
    stdout, stderr, code = HandleTasksList(client, opts, [])
    assert code == 0
    assert "t1" in stdout
    assert len(client.requests) == 1
    assert client.requests[0][0] == "GET"
    assert client.requests[0][1] == "/api/v1/tasks"

def test_handle_tasks_submit():
    client = FakeApiClient()
    client.post_results.append(HttpResult(status=201, body='{"task_id":42}'))
    opts = GlobalOptions()
    args = ["--cmd", "echo", "--arg", "hello", "--env", "KEY=VALUE", "--cpu", "2", "--ram", "256", "--label", "gpu"]
    stdout, stderr, code = HandleTasksSubmit(client, opts, args)
    assert code == 0
    assert "42" in stdout
    assert len(client.requests) == 1
    req = client.requests[0]
    assert req[0] == "POST"
    assert req[1] == "/api/v1/tasks"
    body = json.loads(req[2])
    assert body["command"] == "echo"
    assert body["args"] == ["hello"]
    assert body["env"]["KEY"] == "VALUE"
    assert body["constraints"]["cpu_cores"] == 2
    assert body["constraints"]["ram_mb"] == 256
    assert body["constraints"]["labels"] == ["gpu"]

def test_handle_agents_list():
    client = FakeApiClient()
    client.get_results.append(HttpResult(status=200, body='{"agents":[{"agent_id":"a1","status":"idle"}]}'))
    opts = GlobalOptions()
    stdout, stderr, code = HandleAgentsList(client, opts, [])
    assert code == 0
    assert "a1" in stdout
    assert client.requests[0][1] == "/api/v1/agents"

def test_handle_error_result():
    result = HttpResult(status=400, error="bad request")
    stderr, code = HandleErrorResult(result, verbose=False)
    assert code == 4
    assert "bad request" in stderr

def test_print_key_value_table(capsys):
    rows = [("field1", "value1"), ("field2", "value2")]
    PrintKeyValueTable(rows)
    captured = capsys.readouterr()
    assert "field1" in captured.out
    assert "value2" in captured.out

def test_starts_with():
    assert StartsWith("abcdef", "abc")
    assert not StartsWith("abcdef", "abd")

def test_parse_int():
    assert ParseInt("42") == 42
    assert ParseInt("12x") is None

def test_is_valid_task_id():
    assert IsValidTaskId("123")
    assert not IsValidTaskId("task_1-OK")
    assert not IsValidTaskId("bad id")

def test_ensure_scheme():
    assert EnsureScheme("example.com") == "http://example.com"
    assert EnsureScheme("https://example.com") == "https://example.com"
