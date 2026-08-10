"""Backend adapters, one module per driver.

Nothing here is imported eagerly. :mod:`pydbconnect.registry` maps a backend
name to a ``"module:Class"`` string and imports it on first use, and each
backend imports its driver inside the method that needs it. The practical
consequence: ``import pydbconnect`` works on a machine with no database drivers
installed at all, and a broken Oracle client cannot stop a Postgres job from
starting.

Read :mod:`pydbconnect.backends.base` for the contract and
:mod:`pydbconnect.backends.sqlite` for the reference implementation.
"""

from __future__ import annotations

from .base import Backend

__all__ = ["Backend"]
