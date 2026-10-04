# Pickle4Free

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![Async](https://img.shields.io/badge/async-supported-green.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Status](https://img.shields.io/badge/status-working-brightgreen.svg)

Pure-HTTP async client for **Big Pickle** (`big-pickle`) and the other free models on the official **OpenCode Zen** gateway.

> [!WARNING]
> Built on a reverse-engineered understanding of OpenCode's free-tier gate. The gate has already changed twice (2026-09-17, 2026-09-23); if OpenCode changes it again, requests will fail with `403 FreeTierError` until the gate constants are re-derived.

> [!IMPORTANT]
> Big Pickle is a free stealth model. Per official docs, **prompts and completions may be used to improve the model during the free period**. Don't send anything sensitive.

---

## Overview

* pure-HTTP client — no opencode, no AI SDK, just `aiohttp`
* satisfies the Zen free-tier gate (headers + body conditions, see below)
* SSE streaming with normalized events (`reasoning`, `content`, `tool_call`, `usage`, `cost`, `done`)
* buffered non-streaming API (the wire is always streamed; responses are assembled client-side)
* anonymous by default — no API key required for free models
* system prompts — client-level default, per-call `system_prompt=` override, `system`/`developer` roles in history are merged and prepended

---

## Installation

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

---

## Quick Start

### Chat REPL

```bash
python -m picklepp.client
```

`/system` shows the active system prompt, `/system <text>` sets it, `/system clear` removes it.

### Library

```python
import asyncio
from picklepp import PickleClient

async def main():
    async with PickleClient() as client:
        # buffered (OpenAI-style dict)
        completion = await client.chat([
            {"role": "user", "content": "What is 2+2?"},
        ])
        print(completion["choices"][0]["message"]["content"])
        print(completion.get("usage"), completion.get("cost"))

        # streaming (normalized events)
        async for event in client.stream(
            [{"role": "user", "content": "Explain HTTP in one sentence."}],
            system_prompt="You are a terse network engineer.",   # per-call override
        ):
            if event.type == "content":
                print(event.text, end="", flush=True)

asyncio.run(main())
```

---

## The Free-Tier Gate (reverse-engineered)

Free models on `https://opencode.ai/zen/v1` are rejected with
`403 FreeTierError: "OpenCode's free tier can only be used from within OpenCode"`
unless **all** of the following hold (verified live, 2026-10-01):

| Condition | Detail |
|---|---|
| `User-Agent` | must be `opencode/<version>` with version **≥ 1.18.0** (older → `426 UpgradeRequired`); no upper bound; missing/other → `403` |
| `x-opencode-session` | must match `^ses_[0-9a-f]{12}[0-9A-Za-z]{14}$` — 12 **lowercase hex** chars, then 14 alphanumeric |
| `"stream": true` | mandatory on the wire — non-streaming requests are rejected instantly (the client always streams and buffers locally for `stream: false` UX) |
| `tools` in body | must declare a tool named `bash` **or** `shell` **and** a tool named `read` (names only — schemas are not checked; the client injects dummy definitions automatically) |
| `Authorization` | optional for free models: absent, `Bearer public`, or a real Zen key all work |

Not required (metrics only): `x-opencode-client`, `x-opencode-project`, `x-opencode-request`.

Other verified facts:

* same gate works for the other free models (`mimo-v2.6-flash-free`, `mimo-v2.5-free`, `space-bunny-free`, …)
* paid models need a valid Zen key + balance (`401` without, `Insufficient account funds` without credit)
* anonymous requests are rate-limited per IP; requests with a key are rate-limited per key
* SSE response shape: standard OpenAI chunks with `reasoning_content` deltas, `usage` on chunks, `data: [DONE]`, then a stray `{"choices":[],"cost":"0"}` line (raw line iteration stops at `[DONE]`, buffered mode captures `cost`)

Error types raised by the client:

| Exception | Status | Meaning |
|---|---|---|
| `FreeTierError` | 403 | gate rejected the request (UA/session/stream/tools) |
| `UpgradeRequiredError` | 426 | User-Agent version below the floor |
| `AuthError` | 401 | bad/missing key (paid models); client falls back to anonymous once |
| `RateLimitError` | 429 | rate limited (`.retry_after` set when known); auto-retried |
| `UpstreamError` | 5xx | gateway/upstream failure; auto-retried with backoff |

---

## Configuration

```python
PickleClient(
    base_url="https://opencode.ai/zen/v1",
    api_key=...,            # "auto" (default): $OPENCODE_API_KEY → ~/.local/share/opencode/auth.json → anonymous
                            # None: force anonymous
    user_agent="opencode/1.18.32",
    session_id=None,        # generated once per client (stable → sticky routing affinity)
    model="big-pickle",
    timeout=600.0,
    max_retries=3,
    inject_tools=True,      # add dummy bash/read tools so the gate passes
    system_prompt=None,     # default system instruction for every request
)
```

* `client.chat(messages, model=..., tools=..., system_prompt=..., extra=...)` → OpenAI-style dict (supports `reasoning_content` and `tool_calls`)
* `client.stream(...)` → async iterator of `StreamEvent`
* `client.stream_lines(...)` → raw SSE lines (stops at `[DONE]`)
* `client.models()` → `GET /models`

`system_prompt` semantics: per-call `None` (default) uses the client's `system_prompt`; `""` disables the client default for that call. Any `system`/`developer` messages already present in `messages` are merged with it and sent as a single `system` message prepended to the conversation.

`inject_tools=True` guarantees the gate's `bash|shell` + `read` requirement. The model may genuinely emit `tool_calls` for those names — your app decides whether to execute them or return an error result.

---

## Verification

```bash
.venv/bin/python examples/verify.py
```

Covers: model listing, SSE stream (reasoning/content/finish/done), buffered completion, tool-call emission, multi-turn context, and system-prompt adherence (kwarg + history merge).

---

## License

MIT — see [LICENSE](LICENSE).
