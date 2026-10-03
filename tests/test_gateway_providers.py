"""The two real provider adapters against fake HTTP (G10, G11). No network, no database, no key."""

from __future__ import annotations

import json
from typing import Any

import anthropic
import httpx2 as httpx
import pytest

from architect.gateway.errors import ProviderError
from architect.gateway.providers.anthropic import AnthropicProvider
from architect.gateway.providers.base import ProviderCall, redact
from architect.gateway.providers.openai_compat import OpenAICompatProvider

SCHEMA = {
    "type": "object",
    "properties": {"answer": {"type": "string"}},
    "required": ["answer"],
    "additionalProperties": False,
}


def call(**overrides: Any) -> ProviderCall:
    base: dict[str, Any] = {
        "model": "test-model-id",
        "system": "Be brief.",
        "messages": [{"role": "user", "content": "Capital of France?"}],
        "output_schema": None,
        "max_tokens": 32,
        "temperature": 0.0,
    }
    return ProviderCall(**(base | overrides))


class FakeServer:
    """Records every request and answers with a queue of (status, body) pairs."""

    def __init__(self, *responses: tuple[int, dict[str, Any]]) -> None:
        self.responses = list(responses)
        self.requests: list[httpx.Request] = []

    def transport(self) -> httpx.MockTransport:
        def handle(request: httpx.Request) -> httpx.Response:
            self.requests.append(request)
            status, body = self.responses.pop(0)
            return httpx.Response(status, json=body)

        return httpx.MockTransport(handle)

    def body(self, index: int = -1) -> dict[str, Any]:
        return json.loads(self.requests[index].content)


# --- G10: the OpenAI-compatible provider


def completion(content: str, prompt_tokens: int = 11, completion_tokens: int = 7) -> dict[str, Any]:
    return {
        "choices": [{"index": 0, "message": {"role": "assistant", "content": content}}],
        "usage": {"prompt_tokens": prompt_tokens, "completion_tokens": completion_tokens},
    }


def test_openai_compat_request_shape_and_usage():
    server = FakeServer((200, completion("Paris")))
    provider = OpenAICompatProvider(
        "http://fake-llm:8000/", api_key="compat-secret", transport=server.transport()
    )
    result = provider.complete(call())
    assert (result.text, result.tokens_in, result.tokens_out) == ("Paris", 11, 7)

    request = server.requests[0]
    assert str(request.url) == "http://fake-llm:8000/v1/chat/completions"
    assert request.headers["Authorization"] == "Bearer compat-secret"
    body = server.body()
    assert (
        body["model"] == "test-model-id" and body["max_tokens"] == 32 and body["temperature"] == 0.0
    )
    assert body["messages"] == [
        {"role": "system", "content": "Be brief."},
        {"role": "user", "content": "Capital of France?"},
    ]
    assert "response_format" not in body


def test_openai_compat_structured_output_uses_json_schema():
    server = FakeServer((200, completion('{"answer": "Paris"}')))
    provider = OpenAICompatProvider("http://fake-llm:8000", transport=server.transport())
    result = provider.complete(call(output_schema=SCHEMA, temperature=None))
    assert json.loads(result.text) == {"answer": "Paris"}
    body = server.body()
    assert body["response_format"] == {
        "type": "json_schema",
        "json_schema": {"name": "output", "schema": SCHEMA, "strict": True},
    }
    assert "temperature" not in body
    assert "Authorization" not in server.requests[0].headers


def test_openai_compat_falls_back_to_json_mode_when_the_server_lacks_json_schema():
    server = FakeServer(
        (400, {"error": {"message": "response_format json_schema is not supported"}}),
        (200, completion('{"answer": "Paris"}')),
    )
    provider = OpenAICompatProvider("http://fake-llm:8000", transport=server.transport())
    result = provider.complete(call(output_schema=SCHEMA))
    assert json.loads(result.text) == {"answer": "Paris"}
    assert len(server.requests) == 2
    retry = server.body(1)
    assert retry["response_format"] == {"type": "json_object"}
    assert "matching this schema" in retry["messages"][0]["content"]
    assert json.dumps(SCHEMA, sort_keys=True) in retry["messages"][0]["content"]


@pytest.mark.parametrize(
    ("status", "retryable"),
    [(429, True), (500, True), (503, True), (400, False), (404, False)],
)
def test_openai_compat_maps_statuses(status, retryable):
    server = FakeServer((status, {"error": {"message": "nope"}}))
    provider = OpenAICompatProvider("http://fake-llm:8000", transport=server.transport())
    with pytest.raises(ProviderError) as error:
        provider.complete(call())
    assert error.value.retryable is retryable and error.value.status == status


def test_openai_compat_timeouts_are_retryable():
    def handle(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=request)

    provider = OpenAICompatProvider("http://fake-llm:8000", transport=httpx.MockTransport(handle))
    with pytest.raises(ProviderError) as error:
        provider.complete(call())
    assert error.value.retryable is True and "timed out" in str(error.value)


def test_openai_compat_needs_a_base_url(monkeypatch):
    monkeypatch.delenv("OPENAI_COMPAT_BASE_URL", raising=False)
    with pytest.raises(ValueError):
        OpenAICompatProvider()


def test_redaction():
    assert redact({"Authorization": "Bearer x", "x-api-key": "k", "Accept": "*/*"}) == {
        "Authorization": "<redacted>",
        "x-api-key": "<redacted>",
        "Accept": "*/*",
    }


# --- G11: the Anthropic provider


def message(
    text: str, stop_reason: str = "end_turn", tokens: tuple[int, int] = (13, 5)
) -> dict[str, Any]:
    return {
        "id": "msg_test",
        "type": "message",
        "role": "assistant",
        "model": "test-model-id",
        "content": [{"type": "text", "text": text}],
        "stop_reason": stop_reason,
        "stop_sequence": None,
        "usage": {"input_tokens": tokens[0], "output_tokens": tokens[1]},
    }


def anthropic_provider(server: FakeServer) -> AnthropicProvider:
    client = anthropic.Anthropic(
        api_key="anthropic-test-secret",
        max_retries=0,
        http_client=anthropic.DefaultHttpxClient(transport=server.transport()),
    )
    return AnthropicProvider(client)


def test_anthropic_passes_system_separately_and_parses_usage():
    server = FakeServer((200, message("Paris")))
    result = anthropic_provider(server).complete(call())
    assert (result.text, result.tokens_in, result.tokens_out) == ("Paris", 13, 5)
    request = server.requests[0]
    assert request.url.path == "/v1/messages"
    assert request.headers["x-api-key"] == "anthropic-test-secret"
    body = server.body()
    assert body["system"] == "Be brief." and body["model"] == "test-model-id"
    assert body["messages"] == [{"role": "user", "content": "Capital of France?"}]
    assert body["max_tokens"] == 32 and "temperature" not in body
    assert "output_config" not in body


def test_anthropic_structured_output_uses_the_native_json_schema_format():
    server = FakeServer((200, message('{"answer": "Paris"}')))
    result = anthropic_provider(server).complete(call(output_schema=SCHEMA, temperature=None))
    assert json.loads(result.text) == {"answer": "Paris"}
    body = server.body()
    assert body["output_config"] == {"format": {"type": "json_schema", "schema": SCHEMA}}


@pytest.mark.parametrize(
    ("status", "retryable"),
    [(429, True), (529, True), (500, True), (400, False), (401, False), (404, False)],
)
def test_anthropic_maps_statuses(status, retryable):
    server = FakeServer((status, {"type": "error", "error": {"type": "x", "message": "nope"}}))
    with pytest.raises(ProviderError) as error:
        anthropic_provider(server).complete(call())
    assert error.value.retryable is retryable and error.value.status == status
    assert "anthropic-test-secret" not in str(error.value)


def test_anthropic_refusals_are_not_retried():
    server = FakeServer((200, message("", stop_reason="refusal")))
    with pytest.raises(ProviderError) as error:
        anthropic_provider(server).complete(call())
    assert error.value.retryable is False and "refused" in str(error.value)


def test_anthropic_timeouts_are_retryable():
    def handle(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=request)

    client = anthropic.Anthropic(
        api_key="anthropic-test-secret",
        max_retries=0,
        http_client=anthropic.DefaultHttpxClient(transport=httpx.MockTransport(handle)),
    )
    with pytest.raises(ProviderError) as error:
        AnthropicProvider(client).complete(call())
    assert error.value.retryable is True


# --- the key's variable name (M7): the app's own name first, the conventional one as fallback

APP_KEY = "sk-ant-fake-app-key-for-the-variable-name-test"
SHELL_KEY = "sk-ant-fake-shell-key-for-the-variable-name-test"


def test_anthropic_key_comes_from_the_app_variable_before_the_conventional_one(monkeypatch):
    from architect.gateway.config import ANTHROPIC_KEY_ENVS, anthropic_api_key

    assert ANTHROPIC_KEY_ENVS == ("ARCHITECT_ANTHROPIC_API_KEY", "ANTHROPIC_API_KEY")
    monkeypatch.delenv("ARCHITECT_ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    assert anthropic_api_key() is None

    monkeypatch.setenv("ANTHROPIC_API_KEY", SHELL_KEY)
    assert anthropic_api_key() == SHELL_KEY, "the conventional name is the fallback"
    assert AnthropicProvider()._client.api_key == SHELL_KEY

    monkeypatch.setenv("ARCHITECT_ANTHROPIC_API_KEY", APP_KEY)
    assert anthropic_api_key() == APP_KEY, "the app's own name wins"
    assert AnthropicProvider()._client.api_key == APP_KEY

    monkeypatch.setenv("ARCHITECT_ANTHROPIC_API_KEY", "")
    assert anthropic_api_key() == SHELL_KEY, "an empty app variable does not shadow the fallback"


@pytest.mark.parametrize("name", ["ARCHITECT_ANTHROPIC_API_KEY", "ANTHROPIC_API_KEY"])
def test_default_providers_registers_anthropic_for_either_key_name(monkeypatch, name):
    from architect.gateway.gateway import default_providers

    for variable in ("ARCHITECT_ANTHROPIC_API_KEY", "ANTHROPIC_API_KEY", "OPENAI_COMPAT_BASE_URL"):
        monkeypatch.delenv(variable, raising=False)
    assert set(default_providers()) == {"mock"}
    monkeypatch.setenv(name, APP_KEY)
    providers = default_providers()
    assert set(providers) == {"mock", "anthropic"}
    assert providers["anthropic"]._client.api_key == APP_KEY
