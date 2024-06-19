from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from polars.type_aliases import IntoExpr

from polars_iptools.utils import (get_shared_lib_location, parse_into_expr,
                                  register_plugin)

__all__ = [
    "lookup",
    "lookup_all",
]

lib = get_shared_lib_location()


def lookup(expr: IntoExpr, parallel: bool = False) -> pl.Expr:
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="lookup",
        is_elementwise=True,
        lib=lib,
    )


def lookup_all(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="geoip_lookup_all",
        is_elementwise=True,
        lib=lib,
    )
