from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from polars.type_aliases import IntoExpr

from polars_iptools.utils import (get_shared_lib_location, parse_into_expr,
                                  register_plugin)

__all__ = [
    "asn",
    "full",
]

lib = get_shared_lib_location()


def asn(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="pl_get_asn",
        is_elementwise=True,
        lib=lib,
    )


def full(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="pl_full_geoip",
        is_elementwise=True,
        lib=lib,
    )
