from __future__ import annotations

import asyncio
import json
import os
import random
import re
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional, Sequence, Union

import aiohttp

from .errors import (
    AuthError,
    FreeTierError,
    ProtocolError,
    PickleError,
    RateLimitError,
    UpstreamError,
    UpgradeRequiredError,
)

DEFAULT_BASE_URL = "https://opencode.ai/zen/v1"
DEFAULT_MODEL = "big-pickle"
DEFAULT_USER_AGENT = "opencode/1.18.32"
SESSION_RE = re.compile(r"^ses_[0-9a-f]{12}[0-9A-Za-z]{14}$")
AUTH_JSON = Path.home() / ".local" / "share" / "opencode" / "auth.json"

_AUTO = object()

GATE_TOOLS: List[Dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "bash",
            "description": "Execute a shell command in the workspace.",
            "parameters": {
                "type": "object",
                "properties": {"command": {"type": "string", "description": "The command to execute"}},
                "required": ["command"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read",
            "description": "Read a file from the workspace.",
            "parameters": {
                "type": "object",
                "properties": {"filePath": {"type": "string", "description": "Absolute path to the file"}},
                "required": ["filePath"],
            },
        },
    },
]


def generate_session_id() -> str:
    alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    return "ses_" + secrets.token_hex(6) + "".join(secrets.choice(alphabet) for _ in range(14))


def saved_api_key() -> Optional[str]:
    try:
        data = json.loads(AUTH_JSON.read_text())
        key = data.get("opencode", {}).get("key")
        return key if isinstance(key, str) and key else None
    except (OSError, ValueError, AttributeError):
        return None


def resolve_api_key(api_key: Union[str, None, object] = _AUTO) -> Optional[str]:
    if api_key is _AUTO:
        return os.environ.get("OPENCODE_API_KEY") or saved_api_key()
    if isinstance(api_key, str) and api_key:
        return api_key
    return None


def _tool_name(tool: Dict[str, Any]) -> Optional[str]:
    fn = tool.get("function")
    if isinstance(fn, dict):
        name = fn.get("name")
        return name if isinstance(name, str) else None
    name = tool.get("name")
    return name if isinstance(name, str) else None


def ensure_gate_tools(tools: Optional[Sequence[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    result = list(tools or [])
    names = {n for n in (_tool_name(t) for t in result) if n}
    if not names & {"bash", "shell"}:
        result.append(GATE_TOOLS[0])
    if "read" not in names:
        result.append(GATE_TOOLS[1])
    return result


@dataclass
class StreamEvent:
    type: str
    text: Optional[str] = None
    tool_index: Optional[int] = None
    tool_id: Optional[str] = None
    tool_name: Optional[str] = None
    tool_args: Optional[str] = None
    finish_reason: Optional[str] = None
    usage: Optional[Dict[str, Any]] = None
    cost: Optional[float] = None


class _Assembler:
    def __init__(self) -> None:
        self.id: Optional[str] = None
        self.created: Optional[int] = None
        self.model: Optional[str] = None
        self.reasoning: List[str] = []
        self.content: List[str] = []
        self.tools: Dict[int, Dict[str, Any]] = {}
        self.finish: Optional[str] = None
        self.usage: Optional[Dict[str, Any]] = None
        self.cost: Optional[float] = None
        self.done_seen = False
        self.saw_any = False

    def feed(self, line: str) -> List[StreamEvent]:
        line = line.strip()
        if not line.startswith("data:"):
            return []
        payload = line[5:].strip()
        if payload == "[DONE]":
            self.done_seen = True
            return []
        try:
            chunk = json.loads(payload)
        except ValueError as exc:
            raise ProtocolError(f"invalid SSE JSON: {payload[:200]}") from exc
        if not isinstance(chunk, dict):
            raise ProtocolError(f"unexpected SSE payload: {payload[:200]}")
        self.saw_any = True
        events: List[StreamEvent] = []
        if chunk.get("id") and self.id is None:
            self.id = chunk["id"]
        if chunk.get("created") and self.created is None:
            self.created = chunk["created"]
        if chunk.get("model") and self.model is None:
            self.model = chunk["model"]
        if chunk.get("cost") is not None:
            try:
                self.cost = float(chunk["cost"])
            except (TypeError, ValueError):
                self.cost = None
            events.append(StreamEvent(type="cost", cost=self.cost))
        if isinstance(chunk.get("usage"), dict):
            self.usage = chunk["usage"]
        for choice in chunk.get("choices") or []:
            finish = choice.get("finish_reason")
            if finish:
                self.finish = finish
                events.append(StreamEvent(type="finish", finish_reason=finish))
            delta = choice.get("delta") or {}
            reasoning = delta.get("reasoning_content")
            if reasoning:
                self.reasoning.append(reasoning)
                events.append(StreamEvent(type="reasoning", text=reasoning))
            text = delta.get("content")
            if text:
                self.content.append(text)
                events.append(StreamEvent(type="content", text=text))
            for part in delta.get("tool_calls") or []:
                index = int(part.get("index") or 0)
                entry = self.tools.setdefault(index, {"id": None, "name": None, "args": []})
                fn = part.get("function") or {}
                tool_id = part.get("id")
                if tool_id:
                    entry["id"] = tool_id
                if fn.get("name"):
                    entry["name"] = fn["name"]
                if fn.get("arguments"):
                    entry["args"].append(fn["arguments"])
                events.append(
                    StreamEvent(
                        type="tool_call",
                        tool_index=index,
                        tool_id=tool_id,
                        tool_name=fn.get("name"),
                        tool_args=fn.get("arguments"),
                    )
                )
        return events

    def finish_events(self) -> List[StreamEvent]:
        if not self.saw_any:
            raise ProtocolError("stream ended without any data")
        events: List[StreamEvent] = []
        if self.usage is not None:
            events.append(StreamEvent(type="usage", usage=self.usage))
        events.append(
            StreamEvent(type="done", finish_reason=self.finish, usage=self.usage, cost=self.cost)
        )
        return events

    def tool_calls(self) -> List[Dict[str, Any]]:
        calls = []
        for index in sorted(self.tools):
            entry = self.tools[index]
            calls.append(
                {
                    "id": entry["id"] or f"call_{index}",
                    "type": "function",
                    "function": {"name": entry["name"] or "", "arguments": "".join(entry["args"])},
                }
            )
        return calls

    def completion(self) -> Dict[str, Any]:
        tool_calls = self.tool_calls()
        message: Dict[str, Any] = {"role": "assistant"}
        content = "".join(self.content)
        reasoning = "".join(self.reasoning)
        message["content"] = content if content or not tool_calls else None
        if reasoning:
            message["reasoning_content"] = reasoning
        if tool_calls:
            message["tool_calls"] = tool_calls
        return {
            "id": self.id or "chatcmpl-pickle",
            "object": "chat.completion",
            "created": self.created or 0,
            "model": self.model or DEFAULT_MODEL,
            "choices": [
                {
                    "index": 0,
                    "message": message,
                    "finish_reason": self.finish or "stop",
                }
            ],
            "usage": self.usage,
            "cost": self.cost,
        }


def _content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: List[str] = []
        for part in content:
            if isinstance(part, dict):
                text = part.get("text")
                if isinstance(text, str):
                    parts.append(text)
            elif isinstance(part, str):
                parts.append(part)
        return "\n".join(parts)
    return "" if content is None else str(content)


class PickleClient:
    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE_URL,
        api_key: Union[str, None, object] = _AUTO,
        user_agent: str = DEFAULT_USER_AGENT,
        session_id: Optional[str] = None,
        model: str = DEFAULT_MODEL,
        timeout: float = 600.0,
        max_retries: int = 3,
        inject_tools: bool = True,
        system_prompt: Optional[str] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.user_agent = user_agent
        self.session_id = session_id or generate_session_id()
        if not SESSION_RE.match(self.session_id):
            raise ValueError(f"session_id must match {SESSION_RE.pattern}")
        self.model = model
        self.timeout = timeout
        self.max_retries = max_retries
        self.inject_tools = inject_tools
        self.system_prompt = system_prompt
        self._api_key = resolve_api_key(api_key)
        self._session: Optional[aiohttp.ClientSession] = None
        self._anonymous_fallback = False

    async def __aenter__(self) -> "PickleClient":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    def _client(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    def _headers(self, *, authorize: bool = True) -> Dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
            "User-Agent": self.user_agent,
            "x-opencode-session": self.session_id,
            "x-opencode-client": "cli",
        }
        if authorize and self._api_key and not self._anonymous_fallback:
            headers["Authorization"] = f"Bearer {self._api_key}"
        return headers

    @classmethod
    def _apply_system(
        cls,
        messages: Sequence[Dict[str, Any]],
        system_prompt: Optional[str],
    ) -> List[Dict[str, Any]]:
        texts: List[str] = []
        if system_prompt:
            texts.append(system_prompt)
        rest: List[Dict[str, Any]] = []
        for message in messages:
            if message.get("role") in ("system", "developer"):
                text = _content_text(message.get("content"))
                if text:
                    texts.append(text)
            else:
                rest.append(message)
        if not texts:
            return list(rest)
        if not rest:
            return list(messages)
        return [{"role": "system", "content": "\n".join(texts)}, *rest]

    def _build_body(
        self,
        messages: Sequence[Dict[str, Any]],
        *,
        model: Optional[str],
        tools: Optional[Sequence[Dict[str, Any]]],
        extra: Optional[Dict[str, Any]],
        inject_tools: Optional[bool],
        system_prompt: Optional[str] = None,
    ) -> Dict[str, Any]:
        body: Dict[str, Any] = dict(extra or {})
        body["model"] = model or self.model
        effective_system = self.system_prompt if system_prompt is None else system_prompt
        body["messages"] = self._apply_system(messages, effective_system)
        effective_inject = self.inject_tools if inject_tools is None else inject_tools
        resolved = list(tools or [])
        if effective_inject:
            resolved = ensure_gate_tools(resolved)
        if resolved:
            body["tools"] = resolved
        body["stream"] = True
        return body

    @staticmethod
    def _to_error(status: int, payload: str, headers: Any) -> PickleError:
        message = payload[:500]
        etype = None
        try:
            data = json.loads(payload)
            err = data.get("error") if isinstance(data, dict) else None
            if isinstance(err, dict):
                etype = err.get("type")
                message = err.get("message") or message
            elif isinstance(err, str):
                message = err
        except ValueError:
            pass
        if status == 403 or etype == "FreeTierError":
            return FreeTierError(
                f"{message} (gate requires: User-Agent opencode/>=1.18.0, valid x-opencode-session, stream:true, bash/shell+read tools)",
                status=status,
                body=payload,
            )
        if status == 426 or etype == "UpgradeRequired":
            return UpgradeRequiredError(message, status=status, body=payload)
        if status == 401 or etype == "AuthError":
            return AuthError(message, status=status, body=payload)
        if status == 429 or etype == "RateLimitError":
            retry_after = None
            raw = headers.get("Retry-After") if headers else None
            if raw:
                try:
                    retry_after = float(raw)
                except ValueError:
                    retry_after = None
            return RateLimitError(message, retry_after=retry_after, status=status, body=payload)
        return UpstreamError(message, status=status, body=payload)

    @staticmethod
    def _backoff(attempt: int) -> float:
        return min(0.5 * (2**attempt) + random.random() * 0.25, 8.0)

    async def _post(self, body: Dict[str, Any]) -> aiohttp.ClientResponse:
        url = f"{self.base_url}/chat/completions"
        last: Optional[PickleError] = None
        for attempt in range(self.max_retries + 1):
            try:
                resp = await self._client().post(url, json=body, headers=self._headers())
            except aiohttp.ClientError as exc:
                last = UpstreamError(f"request failed: {exc}")
                if attempt < self.max_retries:
                    await asyncio.sleep(self._backoff(attempt))
                    continue
                raise last from exc
            if resp.status == 200:
                return resp
            payload = await resp.text()
            if resp.status == 401 and self._api_key and not self._anonymous_fallback:
                self._anonymous_fallback = True
                resp.release()
                continue
            error = self._to_error(resp.status, payload, resp.headers)
            resp.release()
            if isinstance(error, RateLimitError) and attempt < self.max_retries:
                await asyncio.sleep(error.retry_after or self._backoff(attempt))
                last = error
                continue
            if isinstance(error, UpstreamError) and error.status and error.status >= 500 and attempt < self.max_retries:
                await asyncio.sleep(self._backoff(attempt))
                last = error
                continue
            raise error
        raise last or UpstreamError("request failed")

    async def _lines(self, body: Dict[str, Any], *, stop_after_done: bool) -> AsyncIterator[str]:
        resp = await self._post(body)
        try:
            async for raw in resp.content:
                line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                if stop_after_done and line.strip() == "data: [DONE]":
                    yield line
                    return
                yield line
        except aiohttp.ClientError as exc:
            raise UpstreamError(f"stream aborted: {exc}") from exc
        finally:
            resp.release()

    async def stream(
        self,
        messages: Sequence[Dict[str, Any]],
        *,
        model: Optional[str] = None,
        tools: Optional[Sequence[Dict[str, Any]]] = None,
        extra: Optional[Dict[str, Any]] = None,
        inject_tools: Optional[bool] = None,
        system_prompt: Optional[str] = None,
    ) -> AsyncIterator[StreamEvent]:
        body = self._build_body(
            messages,
            model=model,
            tools=tools,
            extra=extra,
            inject_tools=inject_tools,
            system_prompt=system_prompt,
        )
        assembler = _Assembler()
        async for line in self._lines(body, stop_after_done=False):
            for event in assembler.feed(line):
                yield event
        for event in assembler.finish_events():
            yield event

    async def stream_lines(
        self,
        messages: Sequence[Dict[str, Any]],
        *,
        model: Optional[str] = None,
        tools: Optional[Sequence[Dict[str, Any]]] = None,
        extra: Optional[Dict[str, Any]] = None,
        inject_tools: Optional[bool] = None,
        system_prompt: Optional[str] = None,
    ) -> AsyncIterator[str]:
        body = self._build_body(
            messages,
            model=model,
            tools=tools,
            extra=extra,
            inject_tools=inject_tools,
            system_prompt=system_prompt,
        )
        async for line in self._lines(body, stop_after_done=True):
            yield line

    async def chat(
        self,
        messages: Sequence[Dict[str, Any]],
        *,
        model: Optional[str] = None,
        tools: Optional[Sequence[Dict[str, Any]]] = None,
        extra: Optional[Dict[str, Any]] = None,
        inject_tools: Optional[bool] = None,
        system_prompt: Optional[str] = None,
    ) -> Dict[str, Any]:
        body = self._build_body(
            messages,
            model=model,
            tools=tools,
            extra=extra,
            inject_tools=inject_tools,
            system_prompt=system_prompt,
        )
        assembler = _Assembler()
        async for line in self._lines(body, stop_after_done=False):
            assembler.feed(line)
        return assembler.completion()

    async def models(self) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/models"
        try:
            async with self._client().get(url, headers=self._headers(authorize=bool(self._api_key))) as resp:
                payload = await resp.text()
                if resp.status != 200:
                    raise self._to_error(resp.status, payload, resp.headers)
                data = json.loads(payload)
        except aiohttp.ClientError as exc:
            raise UpstreamError(f"request failed: {exc}") from exc
        except ValueError as exc:
            raise ProtocolError(f"invalid models payload: {payload[:200]}") from exc
        if isinstance(data, dict) and isinstance(data.get("data"), list):
            return data["data"]
        if isinstance(data, list):
            return data
        raise ProtocolError("unexpected models payload")


async def main() -> None:
    """Interactive chat REPL — run with `python -m picklepp.client`."""
    async with PickleClient() as client:
        history: List[Dict[str, Any]] = []
        print("\n" + "=" * 60)
        print(f"PICKLE CHAT — {client.model}")
        print(f"session: {client.session_id}")
        print("Type 'quit' to exit")
        print("=" * 60)
        if client.system_prompt:
            print(f"system: {client.system_prompt}")
        while True:
            try:
                user_input = (await asyncio.to_thread(input, "\nYou: ")).strip()
            except (EOFError, KeyboardInterrupt):
                print("\nGoodbye!")
                break
            if not user_input:
                continue
            if user_input.lower() in ("quit", "exit", "q"):
                break
            if user_input == "/system" or user_input.startswith("/system "):
                argument = user_input[len("/system") :].strip()
                if not argument:
                    print(f"system: {client.system_prompt or '(none)'}")
                elif argument.lower() in ("clear", "off", "none"):
                    client.system_prompt = None
                    print("system prompt cleared")
                else:
                    client.system_prompt = argument
                    print(f"system prompt set ({len(argument)} chars)")
                continue
            history.append({"role": "user", "content": user_input})
            print("Assistant: ", end="", flush=True)
            reasoning_buf = ""
            answer_started = False
            content_parts: List[str] = []
            tools: Dict[int, Dict[str, str]] = {}
            try:
                async for event in client.stream(history):
                    if event.type == "reasoning":
                        reasoning_buf += event.text or ""
                        print(f"\r[thinking] {reasoning_buf}", end="", flush=True)
                    elif event.type == "content":
                        if not answer_started:
                            answer_started = True
                            print("\r" + " " * 78 + "\r", end="")
                        content_parts.append(event.text or "")
                        print(event.text, end="", flush=True)
                    elif event.type == "tool_call" and event.tool_index is not None:
                        entry = tools.setdefault(event.tool_index, {"name": "", "args": ""})
                        if event.tool_name:
                            entry["name"] = event.tool_name
                        if event.tool_args:
                            entry["args"] += event.tool_args
                print()
            except KeyboardInterrupt:
                print("\n\nInterrupted!")
                history.pop()
                continue
            except Exception as exc:
                print(f"\n[Error] {exc}")
                history.pop()
                continue
            if reasoning_buf and not answer_started:
                print()
            content = "".join(content_parts)
            if tools:
                summary = ", ".join(
                    f"{tools[index]['name']}({tools[index]['args']})" for index in sorted(tools)
                )
                print(f"[tools requested: {summary} — not executed by REPL]")
                if not content:
                    content = f"(requested tools: {summary})"
            history.append({"role": "assistant", "content": content or "(no reply)"})


if __name__ == "__main__":
    asyncio.run(main())
