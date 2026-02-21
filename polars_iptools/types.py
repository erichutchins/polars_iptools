from __future__ import annotations

from typing import Any, Final

import polars as pl


class IPv4(pl.BaseExtension):
    """
    IPv4 Extension Type backing onto UInt32.

    This type represents an IPv4 address stored efficiently as a 32-bit unsigned integer
    but displayed and handled as an IP address.
    """

    def __init__(self) -> None:
        super().__init__(name="polars_iptools.ipv4", storage=pl.UInt32)

    def _string_repr(self) -> str:
        """Concise representation for column headers."""
        return "ipv4"

    def __repr__(self) -> str:
        return "IPv4"

    def __str__(self) -> str:
        return "IPv4"


class IPAddress(pl.BaseExtension):
    """
    Unified IP Address Extension Type backing onto Binary(16).

    This type represents any IP address (IPv4 or IPv6).
    IPv4 addresses are stored as IPv4-mapped IPv6 addresses (::ffff:x.x.x.x).
    """

    def __init__(self) -> None:
        super().__init__(name="polars_iptools.ip_address", storage=pl.Binary)

    def _string_repr(self) -> str:
        """Concise representation for column headers."""
        return "ip_addr"

    def __repr__(self) -> str:
        return "IPAddress"

    def __str__(self) -> str:
        return "IPAddress"


# IP_DTYPES group. Using instances ensures 'dtype in IP_DTYPES' works
# for schema lookups.
IP_DTYPES: Final[frozenset[Any]] = frozenset([IPv4(), IPAddress()])
