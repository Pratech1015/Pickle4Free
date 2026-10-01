from __future__ import annotations

from typing import Any, Optional


class PickleError(Exception):
    def __init__(self, message: str, *, status: Optional[int] = None, body: Any = None) -> None:
        super().__init__(message)
        self.status = status
        self.body = body


class FreeTierError(PickleError):
    """403: request was rejected by the free-tier gate (UA/session/stream/tools mismatch)."""


class UpgradeRequiredError(PickleError):
    """426: User-Agent opencode version is below the supported floor."""


class AuthError(PickleError):
    """401/authorization failure from the gateway."""


class RateLimitError(PickleError):
    """429: rate limited; retry_after in seconds if known."""

    def __init__(self, message: str, *, retry_after: Optional[float] = None, **kwargs: Any) -> None:
        super().__init__(message, **kwargs)
        self.retry_after = retry_after


class UpstreamError(PickleError):
    """5xx or otherwise unexpected gateway/upstream failure."""


class ProtocolError(PickleError):
    """Malformed or unexpected SSE/JSON payload."""
