from __future__ import annotations

from typing import TYPE_CHECKING, Union
from collections.abc import Iterable
import polars as pl

if TYPE_CHECKING:
    from polars.type_aliases import IntoExpr

from polars_iptools.utils import (
    get_shared_lib_location,
    parse_into_expr,
    register_plugin,
)

__all__ = ["is_valid", "is_private", "ipv4_to_numeric", "numeric_to_ipv4", "is_in"]

lib = get_shared_lib_location()


def is_valid(expr: IntoExpr) -> pl.Expr:
    """
    Returns a boolean if string is a valid IPv4 or IPv6 address
    """
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="pl_is_valid",
        is_elementwise=True,
        lib=lib,
    )


def is_private(expr: IntoExpr) -> pl.Expr:
    """
    Returns a boolean if string is an IETF RFC 1918 IPv4 address
    If input is a IPv6 or an invalid IP, this returns False
    """
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="pl_is_private",
        is_elementwise=True,
        lib=lib,
    )


def ipv4_to_numeric(expr: IntoExpr) -> pl.Expr:
    """
    Returns numeric representation (u32) of IPv4 address string
    """
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="pl_ipv4_to_numeric",
        is_elementwise=True,
        lib=lib,
    )


def numeric_to_ipv4(expr: IntoExpr) -> pl.Expr:
    """
    Returns IPv4 address string from its numeric representation
    """
    expr = parse_into_expr(expr)
    # cast to UInt32 and leave any errors as nulls
    expr = expr.cast(pl.UInt32, strict=False)
    return register_plugin(
        args=[expr],
        symbol="pl_numeric_to_ipv4",
        is_elementwise=True,
        lib=lib,
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
    >>> df = pl.DataFrame({'ip': ['8.8.8.8', '1.1.1.1', '2606:4700::1111']})
    >>> networks = ['8.8.8.0/24', '2606:4700::/32']
    >>> df.with_columns(ip.is_in(pl.col('ip'), networks).alias('is_in'))
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

    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr, nets],
        symbol="pl_is_in",
        is_elementwise=True,
        lib=lib,
    )


# todo: mimic this url namespace
# https://github.com/abstractqqq/polars_istr/blob/main/python/polars_istr/url.py#L105
