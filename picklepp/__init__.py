from typing import TYPE_CHECKING, Any

from .errors import (
    AuthError,
    FreeTierError,
    PickleError,
    ProtocolError,
    RateLimitError,
    UpstreamError,
    UpgradeRequiredError,
)

_CLIENT_EXPORTS = frozenset(
    {
        "DEFAULT_BASE_URL",
        "DEFAULT_MODEL",
        "DEFAULT_USER_AGENT",
        "GATE_TOOLS",
        "PickleClient",
        "StreamEvent",
        "ensure_gate_tools",
        "generate_session_id",
        "resolve_api_key",
    }
)

__all__ = [
    "AuthError",
    "DEFAULT_BASE_URL",
    "DEFAULT_MODEL",
    "DEFAULT_USER_AGENT",
    "FreeTierError",
    "GATE_TOOLS",
    "PickleClient",
    "PickleError",
    "ProtocolError",
    "RateLimitError",
    "StreamEvent",
    "UpstreamError",
    "UpgradeRequiredError",
    "ensure_gate_tools",
    "generate_session_id",
    "resolve_api_key",
]


def __getattr__(name: str) -> Any:
    if name in _CLIENT_EXPORTS:
        from . import client

        value = getattr(client, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | _CLIENT_EXPORTS)


if TYPE_CHECKING:
    from .client import (
        DEFAULT_BASE_URL,
        DEFAULT_MODEL,
        DEFAULT_USER_AGENT,
        GATE_TOOLS,
        PickleClient,
        StreamEvent,
        ensure_gate_tools,
        generate_session_id,
        resolve_api_key,
    )
