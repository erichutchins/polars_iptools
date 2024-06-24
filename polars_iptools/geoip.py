from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from polars.type_aliases import IntoExpr

from polars_iptools.utils import (
    get_shared_lib_location,
    parse_into_expr,
    register_plugin,
)

__all__ = [
    "asn",
    "full",
]

lib = get_shared_lib_location()


def asn(expr: IntoExpr) -> pl.Expr:
    """
    Retrieve ASN and Organizational names for Internet-routed IPv4 and IPv6 addresses
    Returns a string in the format "AS{asnum} {asorg}"

    Example
    -------
    >>> import polars as pl
    >>> import polars_iptools as ip

    >>> df = pl.DataFrame({"ip":["8.8.8.8", "192.168.1.1", "2606:4700::1111", "999.abc.def.123"]})
    >>> df.with_columns([ip.geoip.asn(pl.col("ip")).alias("asn")])

    shape: (4, 2)
    ┌─────────────────┬───────────────────────┐
    │ ip              ┆ asn                   │
    │ ---             ┆ ---                   │
    │ str             ┆ str                   │
    ╞═════════════════╪═══════════════════════╡
    │ 8.8.8.8         ┆ AS15169 GOOGLE        │
    │ 192.168.1.1     ┆                       │
    │ 2606:4700::1111 ┆ AS13335 CLOUDFLARENET │
    │ 999.abc.def.123 ┆                       │
    └─────────────────┴───────────────────────┘
    """
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="pl_get_asn",
        is_elementwise=True,
        lib=lib,
    )


def full(expr: IntoExpr) -> pl.Expr:
    """
    Retrieve full ASN and City geolocation metadata of IPv4 and IPv6 addresses
    Returns a struct containing the following fields:

    asnnum
    asnorg
    city
    continent
    country
    country_iso
    latitude
    longitude
    subdivision
    subdivision_iso
    timezone

    Example
    -------
    >>> import polars as pl
    >>> import polars_iptools as ip

    >>> df = pl.DataFrame({"ip":["8.8.8.8", "192.168.1.1", "2606:4700::1111", "999.abc.def.123"]})
    >>> df = df.with_columns([ip.geoip.full(pl.col("ip")).alias("geoip")])

    shape: (4, 2)
    ┌─────────────────┬─────────────────────────────────┐
    │ ip              ┆ geoip                           │
    │ ---             ┆ ---                             │
    │ str             ┆ struct[11]                      │
    ╞═════════════════╪═════════════════════════════════╡
    │ 8.8.8.8         ┆ {15169,"GOOGLE","","NA","","",… │
    │ 192.168.1.1     ┆ {0,"","","","","","","",0.0,0.… │
    │ 2606:4700::1111 ┆ {13335,"CLOUDFLARENET","","","… │
    │ 999.abc.def.123 ┆ {null,null,null,null,null,null… │
    └─────────────────┴─────────────────────────────────┘

    >>> df.schema
    Schema([('ip', String),
            ('geoip',
             Struct({'asnnum': UInt32, 'asnorg': String, 'city': String,
             'continent': String, 'subdivision_iso': String, 'subdivision': String,
             'country_iso': String, 'country': String, 'latitude': Float64,
             'longitude': Float64, 'timezone': String}))])
    """
    expr = parse_into_expr(expr)
    return register_plugin(
        args=[expr],
        symbol="pl_full_geoip",
        is_elementwise=True,
        lib=lib,
    )
