from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, Optional

from aiohttp import web

from .client import DEFAULT_MODEL, PickleClient
from .errors import (
    AuthError,
    FreeTierError,
    PickleError,
    ProtocolError,
    RateLimitError,
    UpstreamError,
    UpgradeRequiredError,
)

_DEFAULT_HOST = os.environ.get("PICKLE_HOST", "127.0.0.1")
_DEFAULT_PORT = int(os.environ.get("PICKLE_PORT", "3017"))

_PASSTHROUGH = {"model", "messages", "stream", "tools"}

_ERROR_STATUS = {
    FreeTierError: 403,
    UpgradeRequiredError: 426,
    AuthError: 401,
    RateLimitError: 429,
    ProtocolError: 502,
    UpstreamError: 502,
}


def _error_payload(exc: PickleError) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "message": str(exc),
        "type": exc.__class__.__name__,
    }
    if exc.status is not None:
        payload["code"] = exc.status
    return {"error": payload}


def _status_for(exc: PickleError) -> int:
    if exc.status and exc.status in (401, 403, 426, 429):
        return exc.status
    return _ERROR_STATUS.get(exc.__class__, 502)


def create_app(client: Optional[PickleClient] = None, **client_kwargs: Any) -> web.Application:
    app = web.Application()
    owned = client is None
    active = client or PickleClient(**client_kwargs)
    app["pickle_client"] = active

    async def shutdown(app: web.Application) -> None:
        if owned:
            await app["pickle_client"].close()

    app.on_shutdown.append(shutdown)

    async def health(request: web.Request) -> web.Response:
        return web.json_response(
            {"status": "ok", "model": active.model, "session": active.session_id, "base_url": active.base_url}
        )

    async def list_models(request: web.Request) -> web.Response:
        try:
            models = await active.models()
        except PickleError as exc:
            return web.json_response(_error_payload(exc), status=_status_for(exc))
        return web.json_response({"object": "list", "data": models})

    async def chat_completions(request: web.Request) -> web.StreamResponse:
        try:
            data = await request.json()
        except ValueError:
            return web.json_response(
                {"error": {"message": "invalid JSON body", "type": "invalid_request_error"}}, status=400
            )
        messages = data.get("messages")
        if not isinstance(messages, list) or not messages:
            return web.json_response(
                {"error": {"message": "messages is required", "type": "invalid_request_error"}}, status=400
            )
        model = data.get("model") or active.model
        tools = data.get("tools") if isinstance(data.get("tools"), list) else None
        extra = {k: v for k, v in data.items() if k not in _PASSTHROUGH}
        stream = bool(data.get("stream", True))

        if not stream:
            try:
                completion = await active.chat(messages, model=model, tools=tools, extra=extra)
            except PickleError as exc:
                return web.json_response(_error_payload(exc), status=_status_for(exc))
            return web.json_response(completion)

        response = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )
        await response.prepare(request)
        try:
            async for line in active.stream_lines(messages, model=model, tools=tools, extra=extra):
                await response.write((line + "\n").encode("utf-8"))
            await response.write(b"\n")
        except PickleError as exc:
            await response.write(("data: " + json.dumps(_error_payload(exc)) + "\n\n").encode("utf-8"))
        except Exception as exc:
            payload = {"error": {"message": str(exc), "type": "internal_error"}}
            await response.write(("data: " + json.dumps(payload) + "\n\n").encode("utf-8"))
        await response.write_eof()
        return response

    app.router.add_get("/health", health)
    app.router.add_get("/v1/models", list_models)
    app.router.add_post("/v1/chat/completions", chat_completions)
    return app


def main(argv: Optional[list] = None) -> None:
    parser = argparse.ArgumentParser(prog="picklepp.server", description="OpenAI-compatible Big-Pickle server")
    parser.add_argument("--host", default=_DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=_DEFAULT_PORT)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--base-url", default=None)
    args = parser.parse_args(argv)
    kwargs: Dict[str, Any] = {"model": args.model}
    if args.base_url:
        kwargs["base_url"] = args.base_url
    app = create_app(**kwargs)
    web.run_app(app, host=args.host, port=args.port, print=lambda text: print(text, flush=True))


if __name__ == "__main__":
    main()
