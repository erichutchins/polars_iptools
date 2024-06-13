from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from polars_iptools.utils import parse_into_expr, register_plugin, parse_version

if TYPE_CHECKING:
    from polars.type_aliases import IntoExpr

if parse_version(pl.__version__) < parse_version("0.20.16"):
    from polars.utils.udfs import _get_shared_lib_location

    lib: str | Path = _get_shared_lib_location(__file__)
else:
    lib = Path(__file__).parent

__all__ = [
    "lookup",
    "lookup_all",
    "is_ip",
    "is_private",
    "ipv4_to_numeric",
    "numeric_to_ipv4"
]


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

def is_ip(expr: IntoExpr) -> pl.Expr:
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="pl_is_ip",
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
