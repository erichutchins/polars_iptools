from __future__ import annotations

from pathlib import Path
from collections.abc import Iterable
from typing import TYPE_CHECKING, Union

import polars as pl
from polars.plugins import register_plugin_function

if TYPE_CHECKING:
    from polars_iptools.typing import IntoExpr

LIB = Path(__file__).parent

__all__ = [
    "is_valid",
    "is_private",
    "ipv4_to_numeric",
    "numeric_to_ipv4",
    "is_in",
    "extract_all_ips",
]


# from https://github.com/erichutchins/geoipsed which also uses rust regex crate
IPV4_PATT = (
    r"""((?:(?:\d|[01]?\d\d|2[0-4]\d|25[0-5])\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d|\d))"""
)
IPV6_PATT = r"""((?:(?:(?:(?:[0-9a-fA-F]){1,4}):){1,4}:[^\s:](?:(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9]).){3,3}(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])))|(?:::(?:ffff(?::0{1,4}){0,1}:){0,1}[^\s:](?:(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9]).){3,3}(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])))|(?:fe80:(?::(?:(?:[0-9a-fA-F]){1,4})){0,4}%[0-9a-zA-Z]{1,})|(?::(?:(?::(?:(?:[0-9a-fA-F]){1,4})){1,7}|:))|(?:(?:(?:[0-9a-fA-F]){1,4}):(?:(?::(?:(?:[0-9a-fA-F]){1,4})){1,6}))|(?:(?:(?:(?:[0-9a-fA-F]){1,4}):){1,2}(?::(?:(?:[0-9a-fA-F]){1,4})){1,5})|(?:(?:(?:(?:[0-9a-fA-F]){1,4}):){1,3}(?::(?:(?:[0-9a-fA-F]){1,4})){1,4})|(?:(?:(?:(?:[0-9a-fA-F]){1,4}):){1,4}(?::(?:(?:[0-9a-fA-F]){1,4})){1,3})|(?:(?:(?:(?:[0-9a-fA-F]){1,4}):){1,5}(?::(?:(?:[0-9a-fA-F]){1,4})){1,2})|(?:(?:(?:(?:[0-9a-fA-F]){1,4}):){1,6}:(?:(?:[0-9a-fA-F]){1,4}))|(?:(?:(?:(?:[0-9a-fA-F]){1,4}):){1,7}:)|(?:(?:(?:(?:[0-9a-fA-F]){1,4}):){7,7}(?:(?:[0-9a-fA-F]){1,4})))"""

ALL_IP_PATT = IPV4_PATT + "|" + IPV6_PATT


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
    # cast to UInt32 and leave any errors as nulls
    expr = pl.select(expr).to_series().cast(pl.UInt32, strict=False)
    return register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="pl_numeric_to_ipv4",
        is_elementwise=True,
    )


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
    if ipv6:
        return expr.str.extract_all(ALL_IP_PATT)
    else:
        return expr.str.extract_all(IPV4_PATT)


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
    This class contains tools for parsing IP addresses.

    Polars Namespace: ip

    Example: df.with_columns([pl.col("srcip").ip.is_private()])
    """

    # noqa: D102
    def __init__(self, expr: pl.Expr):
        self._expr: pl.Expr = expr

    def is_valid(self) -> pl.Expr:
        return is_valid(self._expr)

    def is_private(self) -> pl.Expr:
        return is_private(self._expr)

    def ipv4_to_numeric(self) -> pl.Expr:
        return ipv4_to_numeric(self._expr)

    def numeric_to_ipv4(self) -> pl.Expr:
        return numeric_to_ipv4(self._expr)

    def extract_all_ips(self, ipv6: bool = False) -> pl.Expr:
        return extract_all_ips(self._expr, ipv6)

    def is_in(self, networks: Union[pl.Expr, Iterable[str]]) -> pl.Expr:
        return is_in(self._expr, networks)


@pl.api.register_series_namespace("ip")
class IpSeriesExt:
    """
    This class contains tools for parsing IP addresses.

    Polars Namespace: ip

    Example: df["srcip"].ip.is_private()
    """

    # noqa: D102
    def __init__(self, s: pl.Series):
        self._s: pl.Series = s

    def is_valid(self) -> pl.Series:
        return pl.select(is_valid(self._s)).to_series()

    def is_private(self) -> pl.Series:
        return pl.select(is_private(self._s)).to_series()

    def ipv4_to_numeric(self) -> pl.Series:
        return pl.select(ipv4_to_numeric(self._s)).to_series()

    def numeric_to_ipv4(self) -> pl.Series:
        return pl.select(numeric_to_ipv4(self._s)).to_series()

    def extract_all_ips(self, ipv6: bool = False) -> pl.Series:
        return pl.select(extract_all_ips(self._s, ipv6)).to_series()

    def is_in(self, networks: Union[pl.Expr, Iterable[str]]) -> pl.Series:
        return pl.select(is_in(self._s, networks)).to_series()
