from __future__ import annotations

import asyncio
import json
import sys
from typing import Any, Coroutine, Dict, List

sys.path.insert(0, ".")

from picklepp import PickleClient  # noqa: E402

results: List[tuple] = []


async def check(name: str, coro: Coroutine[Any, Any, Any]) -> Any:
    try:
        value = await coro
        results.append((name, True, ""))
        print(f"PASS  {name}")
        return value
    except Exception as exc:
        results.append((name, False, f"{type(exc).__name__}: {exc}"))
        print(f"FAIL  {name}: {type(exc).__name__}: {exc}")
        return None


async def verify_models(client: PickleClient) -> None:
    models = await client.models()
    ids = {m.get("id") for m in models}
    if "big-pickle" not in ids:
        raise AssertionError(f"big-pickle missing from {sorted(ids)}")


async def verify_stream(client: PickleClient) -> Dict[str, Any]:
    reasoning: List[str] = []
    content: List[str] = []
    finished = None
    seen_done = False
    async for event in client.stream([{"role": "user", "content": "What is 2+2? Reply with just the digit."}]):
        if event.type == "reasoning":
            reasoning.append(event.text or "")
        elif event.type == "content":
            content.append(event.text or "")
        elif event.type == "finish":
            finished = event.finish_reason
        elif event.type == "done":
            seen_done = True
    if not seen_done:
        raise AssertionError("no done event")
    if not "".join(content).strip():
        raise AssertionError("empty content")
    if finished != "stop":
        raise AssertionError(f"finish_reason={finished!r}")
    return {"reasoning": "".join(reasoning), "content": "".join(content)}


async def verify_chat(client: PickleClient) -> Dict[str, Any]:
    completion = await client.chat([{"role": "user", "content": "Say hello in exactly two words."}])
    if completion.get("object") != "chat.completion":
        raise AssertionError(f"bad object: {completion.get('object')!r}")
    message = completion["choices"][0]["message"]
    if not (message.get("content") or "").strip():
        raise AssertionError(f"empty content: {message!r}")
    if completion.get("usage") is None:
        raise AssertionError("missing usage")
    return completion


async def verify_tool_call(client: PickleClient) -> None:
    completion = await client.chat(
        [
            {
                "role": "user",
                "content": "Call the read tool now for the file /tmp/pickle4free-probe.txt. "
                "Do not answer in text, only emit the tool call.",
            }
        ],
        extra={"temperature": 0},
    )
    message = completion["choices"][0]["message"]
    tool_calls = message.get("tool_calls") or []
    if not tool_calls:
        print(f"  note: no tool_calls emitted (finish={completion['choices'][0].get('finish_reason')})")
        return
    call = tool_calls[0]
    if call["function"]["name"] != "read":
        raise AssertionError(f"unexpected tool: {call['function']['name']}")
    json.loads(call["function"]["arguments"])


async def verify_multi_turn(client: PickleClient) -> None:
    first = await client.chat([{"role": "user", "content": "Remember this codeword: ZEBRA-42."}])
    messages = [
        {"role": "user", "content": "Remember this codeword: ZEBRA-42."},
        {"role": "assistant", "content": first["choices"][0]["message"].get("content") or ""},
        {"role": "user", "content": "What codeword did you just see? Reply with only the codeword."},
    ]
    second = await client.chat(messages)
    text = (second["choices"][0]["message"].get("content") or "").upper()
    if "ZEBRA-42" not in text:
        raise AssertionError(f"codeword lost: {text!r}")


async def main() -> int:
    async with PickleClient() as client:
        await check("GET /models lists big-pickle", verify_models(client))
        await check("SSE stream with reasoning/content/finish/done", verify_stream(client))
        await check("buffered chat.completion", verify_chat(client))
        await check("tool_call emission (best effort)", verify_tool_call(client))
        await check("multi-turn context", verify_multi_turn(client))
    failed = [r for r in results if not r[1]]
    print(f"\n{len(results) - len(failed)}/{len(results)} passed")
    for name, _, err in failed:
        print(f"  FAILED: {name}: {err}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
