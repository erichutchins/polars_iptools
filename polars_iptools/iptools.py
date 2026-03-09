from __future__ import annotations

import warnings
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Union

import polars as pl
from polars.plugins import register_plugin_function

from polars_iptools.types import IPAddress, IPv4

if TYPE_CHECKING:
    from polars_iptools.typing import IntoExpr

LIB = Path(__file__).parent

__all__ = [
    "is_valid",
    "is_private",
    "ipv4_to_numeric",
    "numeric_to_ipv4",
    "to_string",
    "to_address",
    "to_ipv4",
    "is_in",
    "extract_ips",
    "extract_public_ips",
    "extract_private_ips",
    "extract_all_ips",
]


def is_valid(expr: IntoExpr) -> pl.Expr:
    """Check whether each string is a valid IPv4 or IPv6 address.

    Parameters
    ----------
    expr
        Expression or column containing IP address strings.

    Returns
    -------
    Expr
        Boolean expression — ``True`` for valid addresses, ``False`` otherwise.

    Examples
    --------
    >>> import polars as pl
    >>> import polars_iptools as ip
    >>> pl.DataFrame({"ip": ["8.8.8.8", "::1", "not_an_ip"]}).with_columns(
    ...     ip.is_valid("ip")
    ... )
    shape: (3, 2)
    ┌───────────┬──────────┐
    │ ip        ┆ ip       │
    │ ---       ┆ ---      │
    │ str       ┆ bool     │
    ╞═══════════╪══════════╡
    │ 8.8.8.8   ┆ true     │
    │ ::1       ┆ true     │
    │ not_an_ip ┆ false    │
    └───────────┴──────────┘
    """
    return register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="pl_is_valid",
        is_elementwise=True,
    )


def is_private(expr: IntoExpr) -> pl.Expr:
    """Check whether each string is an RFC 1918 private IPv4 address.

    Returns ``False`` for IPv6 addresses and invalid strings.

    Parameters
    ----------
    expr
        Expression or column containing IP address strings.

    Returns
    -------
    Expr
        Boolean expression.

    Examples
    --------
    >>> pl.DataFrame({"ip": ["192.168.1.1", "8.8.8.8", "::1"]}).with_columns(
    ...     ip.is_private("ip")
    ... )
    shape: (3, 2)
    ┌─────────────┬────────────┐
    │ ip          ┆ ip         │
    │ ---         ┆ ---        │
    │ str         ┆ bool       │
    ╞═════════════╪════════════╡
    │ 192.168.1.1 ┆ true       │
    │ 8.8.8.8     ┆ false      │
    │ ::1         ┆ false      │
    └─────────────┴────────────┘
    """
    return register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="pl_is_private",
        is_elementwise=True,
    )


def ipv4_to_numeric(expr: IntoExpr) -> pl.Expr:
    """Convert IPv4 address strings to their 32-bit unsigned integer representation.

    Invalid or non-IPv4 strings produce ``null``.

    Parameters
    ----------
    expr
        Expression or column containing IPv4 address strings.

    Returns
    -------
    Expr
        Expression of data type ``UInt32``.

    Examples
    --------
    >>> pl.DataFrame({"ip": ["8.8.8.8", "1.1.1.1"]}).with_columns(
    ...     ip.ipv4_to_numeric("ip")
    ... )
    shape: (2, 2)
    ┌─────────┬───────────┐
    │ ip      ┆ ip        │
    │ ---     ┆ ---       │
    │ str     ┆ u32       │
    ╞═════════╪═══════════╡
    │ 8.8.8.8 ┆ 134744072 │
    │ 1.1.1.1 ┆ 16843009  │
    └─────────┴───────────┘
    """
    return register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="pl_ipv4_to_numeric",
        is_elementwise=True,
    )


def numeric_to_ipv4(expr: IntoExpr) -> pl.Expr:
    """Convert 32-bit unsigned integers to IPv4 address strings.

    Non-numeric or out-of-range values produce ``null``.

    Parameters
    ----------
    expr
        Expression or column containing numeric (``UInt32`` or castable) values.

    Returns
    -------
    Expr
        Expression of data type ``String``.

    Examples
    --------
    >>> pl.DataFrame({"n": [134744072, 16843009]}).with_columns(
    ...     ip.numeric_to_ipv4("n")
    ... )
    shape: (2, 2)
    ┌───────────┬─────────┐
    │ n         ┆ n       │
    │ ---       ┆ ---     │
    │ i64       ┆ str     │
    ╞═══════════╪═════════╡
    │ 134744072 ┆ 8.8.8.8 │
    │ 16843009  ┆ 1.1.1.1 │
    └───────────┴─────────┘
    """
    if isinstance(expr, str):
        expr = pl.col(expr)
    elif isinstance(expr, pl.Series):
        expr = pl.lit(expr)

    # cast to UInt32 and leave any errors as nulls
    expr = expr.cast(pl.UInt32, strict=False)

    return register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="pl_numeric_to_ipv4",
        is_elementwise=True,
    )


def to_string(expr: IntoExpr) -> pl.Expr:
    """Convert an IPv4 or IPAddress extension column back to canonical string form.

    Accepts ``IPv4`` (``UInt32`` storage) or ``IPAddress`` (``Binary`` storage)
    extension columns. IPv4-mapped IPv6 addresses (``::ffff:x.x.x.x``) are
    rendered as plain IPv4 strings.

    Parameters
    ----------
    expr
        Expression or column of ``IPv4`` or ``IPAddress`` extension type.
        Pass ``expr.ext.storage()`` if working with raw storage.

    Returns
    -------
    Expr
        Expression of data type ``String``.

    Examples
    --------
    >>> df = pl.DataFrame({"ip": ["8.8.8.8", "2606:4700::1111"]})
    >>> df.with_columns(ip.to_address("ip").ip.to_string())
    shape: (2, 2)
    ┌─────────────────┬─────────────────┐
    │ ip              ┆ ip              │
    │ ---             ┆ ---             │
    │ str             ┆ str             │
    ╞═════════════════╪═════════════════╡
    │ 8.8.8.8         ┆ 8.8.8.8         │
    │ 2606:4700::1111 ┆ 2606:4700::1111 │
    └─────────────────┴─────────────────┘
    """
    if isinstance(expr, str):
        expr = pl.col(expr)
    elif isinstance(expr, pl.Series):
        expr = pl.lit(expr)

    return register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="pl_ip_to_str",
        is_elementwise=True,
    )


def to_ipv4(expr: IntoExpr) -> pl.Expr:
    """Parse IPv4 address strings into the ``IPv4`` extension type (``UInt32`` storage).

    The ``IPv4`` type is the most storage-efficient representation for IPv4-only
    datasets — 4 bytes per address vs. ~9–15 bytes as a string. The type is
    preserved through Parquet and IPC round-trips.

    Invalid strings produce ``null``. IPv6 addresses are not supported; use
    :func:`to_address` for mixed IPv4/IPv6 data.

    Parameters
    ----------
    expr
        Expression or column containing IPv4 address strings.

    Returns
    -------
    Expr
        Expression of extension type ``IPv4`` (``UInt32`` storage).

    Examples
    --------
    >>> df = pl.DataFrame({"ip": ["8.8.8.8", "192.168.1.1"]})
    >>> df.with_columns(ip.to_ipv4("ip"))
    shape: (2, 2)
    ┌─────────────┬─────────────┐
    │ ip          ┆ ip          │
    │ ---         ┆ ---         │
    │ str         ┆ ipv4        │
    ╞═════════════╪═════════════╡
    │ 8.8.8.8     ┆ 8.8.8.8     │
    │ 192.168.1.1 ┆ 192.168.1.1 │
    └─────────────┴─────────────┘
    """
    if isinstance(expr, str):
        expr = pl.col(expr)
    elif isinstance(expr, pl.Series):
        expr = pl.lit(expr)

    out = register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="pl_ipv4_from_str",
        is_elementwise=True,
    )
    return out.ext.to(IPv4())


def to_address(expr: IntoExpr) -> pl.Expr:
    """Promote strings, integers, or binary to the ``IPAddress`` extension type.

    ``IPAddress`` uses 16-byte binary storage (network-order IPv6). IPv4 addresses
    are stored as IPv4-mapped IPv6 (``::ffff:x.x.x.x``). This is the recommended
    type for mixed IPv4/IPv6 datasets and for any data that will be written to
    Parquet or IPC — the extension type metadata is preserved on read.

    Accepts:

    - ``String`` — parsed as IPv4 or IPv6
    - ``UInt32`` — treated as IPv4 numeric
    - ``Binary`` (16 bytes) — used as-is

    Parameters
    ----------
    expr
        Expression or column containing IP addresses.

    Returns
    -------
    Expr
        Expression of extension type ``IPAddress`` (``Binary`` storage).

    Examples
    --------
    >>> df = pl.DataFrame({"ip": ["8.8.8.8", "2606:4700::1111", "192.168.1.1"]})
    >>> df.with_columns(ip.to_address("ip"))
    shape: (3, 2)
    ┌─────────────────┬─────────────────┐
    │ ip              ┆ ip              │
    │ ---             ┆ ---             │
    │ str             ┆ ip_addr         │
    ╞═════════════════╪═════════════════╡
    │ 8.8.8.8         ┆ 8.8.8.8         │
    │ 2606:4700::1111 ┆ 2606:4700::1111 │
    │ 192.168.1.1     ┆ 192.168.1.1     │
    └─────────────────┴─────────────────┘
    """
    if isinstance(expr, str):
        expr = pl.col(expr)
    elif isinstance(expr, pl.Series):
        expr = pl.lit(expr)

    out = register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="pl_to_ip",
        is_elementwise=True,
    )
    return out.ext.to(IPAddress())


def extract_ips(
    expr: IntoExpr,
    ipv6: bool = False,
    only_public: bool = False,
    ignore_private: bool = False,
    ignore_loopback: bool = False,
    ignore_broadcast: bool = False,
) -> pl.Expr:
    """
    Extract IP addresses from text, including defanged IPs (e.g. 192[.]168[.]1[.]1).

    Parameters
    ----------
    expr
        Expression or column containing text to extract IPs from.
    ipv6
        If True, also extract IPv6 addresses.
    only_public
        If True, skip private, loopback, and broadcast addresses.
    ignore_private
        If True, skip RFC 1918 (IPv4) and ULA (IPv6) addresses.
    ignore_loopback
        If True, skip loopback addresses (127.0.0.0/8, ::1).
    ignore_broadcast
        If True, skip broadcast addresses (255.255.255.255).

    Returns
    -------
    Expr
        Expression of data type ``List(String)``.
    """
    if isinstance(expr, str):
        expr = pl.col(expr)
    elif isinstance(expr, pl.Series):
        expr = pl.lit(expr)

    return register_plugin_function(
        args=[
            expr,
            pl.lit(ipv6),
            pl.lit(only_public),
            pl.lit(ignore_private),
            pl.lit(ignore_loopback),
            pl.lit(ignore_broadcast),
        ],
        plugin_path=LIB,
        function_name="pl_extract_ips",
        is_elementwise=True,
    )


def extract_public_ips(expr: IntoExpr, ipv6: bool = False) -> pl.Expr:
    """Extract only publicly routable IP addresses from text.

    Shortcut for ``extract_ips(expr, only_public=True)``. Skips RFC 1918
    private ranges, loopback (``127.0.0.0/8``, ``::1``), and broadcast
    (``255.255.255.255``). Defanged IPs (e.g. ``192[.]168[.]1[.]1``) are
    handled automatically.

    Parameters
    ----------
    expr
        Expression or column containing text to extract IPs from.
    ipv6
        If ``True``, also extract IPv6 addresses.

    Returns
    -------
    Expr
        Expression of data type ``List(String)``.

    Examples
    --------
    >>> pl.DataFrame({"text": ["seen 8.8.8.8 and 192.168.1.1"]}).with_columns(
    ...     ip.extract_public_ips("text")
    ... )
    shape: (1, 2)
    ┌───────────────────────────────┬──────────────┐
    │ text                          ┆ text         │
    │ ---                           ┆ ---          │
    │ str                           ┆ list[str]    │
    ╞═══════════════════════════════╪══════════════╡
    │ seen 8.8.8.8 and 192.168.1.1  ┆ ["8.8.8.8"] │
    └───────────────────────────────┴──────────────┘
    """
    return extract_ips(expr, ipv6=ipv6, only_public=True)


def extract_private_ips(expr: IntoExpr, ipv6: bool = False) -> pl.Expr:
    """Extract only private IP addresses from text.

    Returns RFC 1918 addresses (``10/8``, ``172.16/12``, ``192.168/16``) for
    IPv4, and ULA addresses (``fc00::/7``) for IPv6. Implemented as a
    post-extraction filter — the extractor first finds all IPs, then keeps
    only those that pass ``Ipv4Addr::is_private()`` / ULA check.

    Parameters
    ----------
    expr
        Expression or column containing text to extract IPs from.
    ipv6
        If ``True``, also extract private IPv6 (ULA) addresses.

    Returns
    -------
    Expr
        Expression of data type ``List(String)``.

    Examples
    --------
    >>> pl.DataFrame({"text": ["8.8.8.8 and 10.0.0.1 and 192.168.1.1"]}).with_columns(
    ...     ip.extract_private_ips("text")
    ... )
    shape: (1, 2)
    ┌───────────────────────────────────────┬──────────────────────────────┐
    │ text                                  ┆ text                         │
    │ ---                                   ┆ ---                          │
    │ str                                   ┆ list[str]                    │
    ╞═══════════════════════════════════════╪══════════════════════════════╡
    │ 8.8.8.8 and 10.0.0.1 and 192.168.1.1 ┆ ["10.0.0.1", "192.168.1.1"] │
    └───────────────────────────────────────┴──────────────────────────────┘
    """
    if isinstance(expr, str):
        expr = pl.col(expr)
    elif isinstance(expr, pl.Series):
        expr = pl.lit(expr)

    return register_plugin_function(
        args=[expr, pl.lit(ipv6)],
        plugin_path=LIB,
        function_name="pl_extract_private_ips",
        is_elementwise=True,
    )


def extract_all_ips(expr: IntoExpr, ipv6: bool = False, **kwargs) -> pl.Expr:
    """Deprecated: use :func:`extract_ips` instead."""
    warnings.warn(
        "extract_all_ips() is deprecated, use extract_ips() instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return extract_ips(expr, ipv6=ipv6, **kwargs)


def is_in(expr: IntoExpr, networks: Union[pl.Expr, Iterable[str]]) -> pl.Expr:
    """
    Returns a boolean if IPv4 or IPv6 address is in any of the network ranges in "networks"

    Parameters
    ----------
    expr
        The expression or column containing the IP addresses to check
    networks
        IPv4 and IPv6 CIDR ranges defining the network. This can be a Polars expression, a list of strings, or a set of strings.

    Examples
    --------
    >>> import polars as pl
    >>> import polars_iptools as ip
    >>> df = pl.DataFrame({"ip": ["8.8.8.8", "1.1.1.1", "2606:4700::1111"]})
    >>> networks = ["8.8.8.0/24", "2606:4700::/32"]
    >>> df.with_columns(ip.is_in(pl.col("ip"), networks).alias("is_in"))
    shape: (3, 2)
    ┌─────────────────┬───────┐
    │ ip              ┆ is_in │
    │ ---             ┆ ---   │
    │ str             ┆ bool  │
    ╞═════════════════╪═══════╡
    │ 8.8.8.8         ┆ true  │
    │ 1.1.1.1         ┆ false │
    │ 2606:4700::1111 ┆ true  │
    └─────────────────┴───────┘
    """
    if isinstance(networks, pl.Expr):
        nets = networks
    elif isinstance(networks, Iterable) and not isinstance(networks, str):
        nets = pl.lit(pl.Series(values=list(networks), dtype=pl.Utf8))
    else:
        # generic iterable
        raise TypeError("networks must be a polars Expr or an iterable of strings")

    nets = nets.unique().drop_nulls()

    return register_plugin_function(
        args=[expr, nets],
        plugin_path=LIB,
        function_name="pl_is_in",
        is_elementwise=True,
    )


@pl.api.register_expr_namespace("ip")
class IpExprExt:
    """IP address operations available via the ``.ip`` expression namespace.

    All functions in this module are also available as standalone functions.
    The ``.ip`` namespace is a convenience layer — e.g.:

    .. code-block:: python

        # Standalone
        ip.to_address(pl.col("src"))

        # Namespace
        pl.col("src").ip.to_address()

    Parameters
    ----------
    expr
        The Polars expression this namespace is attached to.
    """

    def __init__(self, expr: pl.Expr):
        self._expr: pl.Expr = expr

    def is_valid(self) -> pl.Expr:
        return is_valid(self._expr)

    def is_private(self) -> pl.Expr:
        return is_private(self._expr)

    def to_ipv4(self) -> pl.Expr:
        """Convert/Parse to IPv4 extension type (optimized 32-bit)."""
        return to_ipv4(self._expr)

    def to_address(self) -> pl.Expr:
        """Promote to Unified IPAddress extension type (future-proof)."""
        return to_address(self._expr)

    def to_native(self) -> pl.Expr:
        """Alias for to_address()."""
        return self.to_address()

    def to_string(self) -> pl.Expr:
        """Convert IP extension back to a canonical string representation."""
        return to_string(self._expr.ext.storage())

    def to_canonical(self) -> pl.Expr:
        """Alias for to_string()."""
        return self.to_string()

    def ipv4_to_numeric(self) -> pl.Expr:
        """Deprecated: use ``ipv4_to_numeric(expr)`` standalone function instead."""
        warnings.warn(
            "IpExprExt.ipv4_to_numeric() is deprecated, use ip.ipv4_to_numeric(expr) instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return ipv4_to_numeric(self._expr)

    def numeric_to_ipv4(self) -> pl.Expr:
        """Deprecated: use ``numeric_to_ipv4(expr)`` standalone function instead."""
        warnings.warn(
            "IpExprExt.numeric_to_ipv4() is deprecated, use ip.numeric_to_ipv4(expr) instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return numeric_to_ipv4(self._expr)

    def extract_ips(self, ipv6: bool = False, **kwargs) -> pl.Expr:
        return extract_ips(self._expr, ipv6=ipv6, **kwargs)

    def extract_public_ips(self, ipv6: bool = False) -> pl.Expr:
        return extract_public_ips(self._expr, ipv6=ipv6)

    def extract_private_ips(self, ipv6: bool = False) -> pl.Expr:
        return extract_private_ips(self._expr, ipv6=ipv6)

    def extract_all_ips(self, ipv6: bool = False) -> pl.Expr:
        """Deprecated: use :func:`extract_ips` instead."""
        warnings.warn(
            "IpExprExt.extract_all_ips() is deprecated, use extract_ips() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return extract_all_ips(self._expr, ipv6)

    def is_in(self, networks: Union[pl.Expr, Iterable[str]]) -> pl.Expr:
        return is_in(self._expr, networks)


@pl.api.register_series_namespace("ip")
class IpSeriesExt:
    """IP address operations available via the ``.ip`` Series namespace.

    Mirrors :class:`IpExprExt` for direct Series access — e.g.:

    .. code-block:: python

        series.ip.to_address()
        series.ip.extract_ips()

    Parameters
    ----------
    s
        The Polars Series this namespace is attached to.
    """

    def __init__(self, s: pl.Series):
        self._s: pl.Series = s

    def is_valid(self) -> pl.Series:
        return pl.select(is_valid(self._s)).to_series()

    def is_private(self) -> pl.Series:
        return pl.select(is_private(self._s)).to_series()

    def to_ipv4(self) -> pl.Series:
        return pl.select(to_ipv4(self._s)).to_series()

    def to_address(self) -> pl.Series:
        return pl.select(to_address(self._s)).to_series()

    def to_string(self) -> pl.Series:
        return pl.select(to_string(pl.lit(self._s).ext.storage())).to_series()

    def to_canonical(self) -> pl.Series:
        """Alias for to_string()."""
        return self.to_string()

    def ipv4_to_numeric(self) -> pl.Series:
        """Deprecated: use ``ipv4_to_numeric(expr)`` standalone function instead."""
        warnings.warn(
            "IpSeriesExt.ipv4_to_numeric() is deprecated, use ip.ipv4_to_numeric(expr) instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return pl.select(ipv4_to_numeric(self._s)).to_series()

    def numeric_to_ipv4(self) -> pl.Series:
        """Deprecated: use ``numeric_to_ipv4(expr)`` standalone function instead."""
        warnings.warn(
            "IpSeriesExt.numeric_to_ipv4() is deprecated, use ip.numeric_to_ipv4(expr) instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return pl.select(numeric_to_ipv4(self._s)).to_series()

    def extract_ips(self, ipv6: bool = False, **kwargs) -> pl.Series:
        return pl.select(extract_ips(self._s, ipv6=ipv6, **kwargs)).to_series()

    def extract_public_ips(self, ipv6: bool = False) -> pl.Series:
        return pl.select(extract_public_ips(self._s, ipv6=ipv6)).to_series()

    def extract_private_ips(self, ipv6: bool = False) -> pl.Series:
        return pl.select(extract_private_ips(self._s, ipv6=ipv6)).to_series()

    def extract_all_ips(self, ipv6: bool = False) -> pl.Series:
        """Deprecated: use :func:`extract_ips` instead."""
        warnings.warn(
            "IpSeriesExt.extract_all_ips() is deprecated, use extract_ips() instead",
            DeprecationWarning,
            stacklevel=2,
        )
        return pl.select(extract_all_ips(self._s, ipv6)).to_series()

    def is_in(self, networks: Union[pl.Expr, Iterable[str]]) -> pl.Series:
        return pl.select(is_in(self._s, networks)).to_series()
