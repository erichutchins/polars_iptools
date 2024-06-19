from __future__ import annotations

from typing import TYPE_CHECKING, List, Union

import polars as pl

if TYPE_CHECKING:
    from polars.type_aliases import IntoExpr

from polars_iptools.utils import (get_shared_lib_location, parse_into_expr,
                                  register_plugin)

__all__ = ["is_valid", "is_private", "ipv4_to_numeric", "numeric_to_ipv4", "is_in"]

lib = get_shared_lib_location()


def is_valid(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="pl_is_valid",
        is_elementwise=True,
        lib=lib,
    )


def is_private(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="pl_is_private",
        is_elementwise=True,
        lib=lib,
    )


def ipv4_to_numeric(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="pl_ipv4_to_numeric",
        is_elementwise=True,
        lib=lib,
    )


def numeric_to_ipv4(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="pl_numeric_to_ipv4",
        is_elementwise=True,
        lib=lib,
    )


def is_in(expr: IntoExpr, networks: Union[pl.Expr, List[str]]) -> pl.Expr:
    if isinstance(networks, pl.Expr):
        nets = networks.unique().drop_nulls()
    else:
        nets = pl.Series(values=networks, dtype=pl.String).unique().drop_nulls()

    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr, nets],
        symbol="pl_is_in",
        is_elementwise=True,
        lib=lib,
    )


# todo: mimic this url namespace
# https://github.com/abstractqqq/polars_istr/blob/main/python/polars_istr/url.py#L105
