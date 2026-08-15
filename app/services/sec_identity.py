"""SEC identity configuration and validation."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class SecIdentityStatus:
    """SEC User-Agent configuration status."""

    configured: bool
    message: str


def get_sec_user_agent() -> str | None:
    """Return a configured SEC User-Agent or None if synchronization should be disabled."""
    value = os.getenv("SEC_USER_AGENT", "").strip()
    if not value:
        return None
    if not is_valid_user_agent(value):
        return None
    return value


def is_valid_user_agent(value: str) -> bool:
    """Validate that the SEC User-Agent looks honest and contactable."""
    if len(value) < 12 or len(value) > 180:
        return False
    if "@" not in value:
        return False
    return re.search(r"[^@\s]+@[^@\s]+\.[^@\s]+", value) is not None


def sec_identity_status() -> SecIdentityStatus:
    """Return a safe, non-secret status for Settings."""
    raw = os.getenv("SEC_USER_AGENT", "").strip()
    if not raw:
        return SecIdentityStatus(False, "SEC synchronization is disabled until SEC_USER_AGENT is configured.")
    if not is_valid_user_agent(raw):
        return SecIdentityStatus(False, "SEC_USER_AGENT is present but must include an application name and contact email.")
    return SecIdentityStatus(True, "SEC synchronization is configured.")
