from __future__ import annotations

from typing import Any, Final

import polars as pl


class IPv4(pl.BaseExtension):
    """
    IPv4 Extension Type backing onto UInt32.

    This type represents an IPv4 address stored efficiently as a 32-bit unsigned integer
    but displayed and handled as an IP address.

    Known issues
    ------------
    - All-null columns panic when wrapped into extension types
      (https://github.com/pola-rs/polars/issues/25322, polars-expr/dispatch/extension.rs).
      Include at least one valid value to avoid this.
    - Custom display formatting (showing ``8.8.8.8`` instead of raw ``u32``) is pending
      upstream support (https://github.com/pola-rs/polars/pull/26649).
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

    # TODO: implement dyn_display_value for human-readable IPv4 display
    # once https://github.com/pola-rs/polars/pull/26649 is merged.
    # See https://github.com/deanm0000/uuid_pl_extension for reference.


class IPAddress(pl.BaseExtension):
    """
    Unified IP Address Extension Type backing onto Binary(16).

    This type represents any IP address (IPv4 or IPv6).
    IPv4 addresses are stored as IPv4-mapped IPv6 addresses (::ffff:x.x.x.x).

    Known issues
    ------------
    - All-null columns panic when wrapped into extension types
      (https://github.com/pola-rs/polars/issues/25322, polars-expr/dispatch/extension.rs).
      Include at least one valid value to avoid this.
    - Custom display formatting (showing ``8.8.8.8`` instead of raw bytes) is pending
      upstream support (https://github.com/pola-rs/polars/pull/26649).
    - ``to_list()`` crashes on ``list[extension]`` columns
      (https://github.com/pola-rs/polars/issues/19418).
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

    # TODO: implement dyn_display_value for human-readable IP display
    # once https://github.com/pola-rs/polars/pull/26649 is merged.
    # See https://github.com/deanm0000/uuid_pl_extension for reference.


# IP_DTYPES group. Using instances ensures 'dtype in IP_DTYPES' works
# for schema lookups.
IP_DTYPES: Final[frozenset[Any]] = frozenset([IPv4(), IPAddress()])
