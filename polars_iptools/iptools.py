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
    "extract_all_ips",
]


def is_valid(expr: IntoExpr) -> pl.Expr:
    """
    Returns a boolean if string is a valid IPv4 or IPv6 address
    """
    return register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="pl_is_valid",
        is_elementwise=True,
    )


def is_private(expr: IntoExpr) -> pl.Expr:
    """
    Returns a boolean if string is an IETF RFC 1918 IPv4 address
    If input is a IPv6 or an invalid IP, this returns False
    """
    return register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="pl_is_private",
        is_elementwise=True,
    )


def ipv4_to_numeric(expr: IntoExpr) -> pl.Expr:
    """
    Returns numeric representation (u32) of IPv4 address string
    """
    return register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="pl_ipv4_to_numeric",
        is_elementwise=True,
    )


def numeric_to_ipv4(expr: IntoExpr) -> pl.Expr:
    """
    Returns IPv4 address string from its numeric representation
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
    """
    Convert IP extension column (IPv4 or IPAddress) to a string column.
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
    """
    Parse string columns into the IPv4 extension type (UInt32 storage).

    This is the most storage-efficient type for IPv4-only datasets.
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
    """
    Promote Strings, Numbers, or Binary to the Unified IPAddress extension type (Binary storage).

    This is the modern default for handling mixed IPv4/IPv6 networks.
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


def extract_all_ips(expr: IntoExpr, ipv6: bool = False) -> pl.Expr:
    """
    Extract all IP addresses (convience wrapper for str.extract_all)

    Note: this is purely a regex match and not a semantic validation of
    the string as a true IP address. A common pitfall, for example, is the
    version string in a browser useragent, as shown below.

    Parameters
    ----------
    expr
        The expression or column containing string to extract IP addresses
    ipv6: bool
        If true, look for both ipv4 and ipv6 candidate strings

    Examples
    --------
    >>> import polars as pl
    >>> import polars_iptools as ip
    >>>
    >>> df = pl.DataFrame(
    ...     {
    ...         "log": [
    ...             'test: 8.8.8.8, "180.179.174.219" 1.2.3.4.5',
    ...             "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.3",
    ...         ]
    ...     }
    ... )
    >>>
    >>> with pl.Config(fmt_str_lengths=100):
    ...     print(df.with_columns(ip.extract_all_ips(pl.col("log")).alias("ips")))
    shape: (2, 2)
    ┌───────────────────────────────────────────────────────────────────────────────────────────────────────┬───────────────────────────────────────────┐
    │ log                                                                                                   ┆ ips                                       │
    │ ---                                                                                                   ┆ ---                                       │
    │ str                                                                                                   ┆ list[str]                                 │
    ╞═══════════════════════════════════════════════════════════════════════════════════════════════════════╪═══════════════════════════════════════════╡
    │ test: 8.8.8.8, "180.179.174.219" 1.2.3.4.5                                                            ┆ ["8.8.8.8", "180.179.174.219", "1.2.3.4"] │
    │ Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Sa… ┆ ["120.0.0.0"]                             │
    └───────────────────────────────────────────────────────────────────────────────────────────────────────┴───────────────────────────────────────────┘

    Returns
    -------
    Expr
        Expression of data type `List(String)`.
    """
    # Convert to a polars expression if not already one
    if isinstance(expr, str):
        expr = pl.col(expr)
    elif isinstance(expr, pl.Series):
        expr = pl.lit(expr)

    return register_plugin_function(
        args=[expr, pl.lit(ipv6)],
        plugin_path=LIB,
        function_name="pl_extract_all_ips",
        is_elementwise=True,
    )


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
    """
    Namespace for IP address operations in Polars expressions.

    Example: pl.col("src_ip").ip.to_address()
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

    def extract_all_ips(self, ipv6: bool = False) -> pl.Expr:
        return extract_all_ips(self._expr, ipv6)

    def is_in(self, networks: Union[pl.Expr, Iterable[str]]) -> pl.Expr:
        return is_in(self._expr, networks)


@pl.api.register_series_namespace("ip")
class IpSeriesExt:
    """
    Namespace for IP address operations on Polars Series.

    Example: df["srcip"].ip.is_private()
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

    def extract_all_ips(self, ipv6: bool = False) -> pl.Series:
        return pl.select(extract_all_ips(self._s, ipv6)).to_series()

    def is_in(self, networks: Union[pl.Expr, Iterable[str]]) -> pl.Series:
        return pl.select(is_in(self._s, networks)).to_series()
