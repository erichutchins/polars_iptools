from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
from polars.plugins import register_plugin_function

if TYPE_CHECKING:
    from polars_iptools.typing import IntoExpr

LIB = Path(__file__).parent


__all__ = [
    "asn",
    "full",
]


def asn(expr: IntoExpr, reload_mmdb: bool = False) -> pl.Expr:
    """
    Retrieve ASN and Organizational names for Internet-routed IPv4 and IPv6 addresses
    Returns a string in the format "AS{asnum} {asorg}"

    Parameters
    ----------
    expr
        The expression or column containing IP addresses.
    reload_mmdb : bool, optional
        Force reload/reinitialize of MaxMind db readers. Default is False.

    Returns
    -------
    pl.Expr
        Expression of :class:`Utf8` strings

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
    Notes
    -----
    - Invalid IP address strings or IPs not found in the database will result in an empty string output.
    """
    return register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="pl_get_asn",
        kwargs={
            "reload_mmdb": reload_mmdb,
        },
        is_elementwise=True,
    )


def full(expr: IntoExpr, reload_mmdb: bool = False) -> pl.Expr:
    """
    Retrieve full ASN and City geolocation metadata of IPv4 and IPv6 addresses

    Parameters
    ----------
    expr
        The expression or column containing IP addresses.
    reload_mmdb : bool, optional
        Force reload/reinitialize of MaxMind db readers. Default is False.

    Returns
    -------
    pl.Expr
        An expression that returns a struct containing the following fields:
        - asnnum : UInt32
        - asnorg : String
        - city : String
        - continent : String
        - country : String
        - country_iso : String
        - latitude : Float64
        - longitude : Float64
        - subdivision : String
        - subdivision_iso : String
        - timezone : String
        - postalcode: String

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
    │ str             ┆ struct[12]                      │
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
             'longitude': Float64, 'timezone': String, 'postalcode': String}))])
    Notes
    -----
    - IP addresses that are invalid or not found in the database will result in `null` values in the respective fields.
    """
    return register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="pl_full_geoip",
        kwargs={
            "reload_mmdb": reload_mmdb,
        },
        is_elementwise=True,
    )


@pl.api.register_expr_namespace("geoip")
class GeoIpExprExt:
    """
    This class contains tools for geolocation enrichment of IP addresses.

    Polars Namespace: geoip

    Example: df.with_columns([pl.col("srcip").geoip.asn()])
    """

    # noqa: D102
    def __init__(self, expr: pl.Expr):
        self._expr: pl.Expr = expr

    def asn(self, reload_mmdb: bool = False) -> pl.Expr:
        return asn(self._expr, reload_mmdb=reload_mmdb)

    def full(self, reload_mmdb: bool = False) -> pl.Expr:
        return full(self._expr, reload_mmdb=reload_mmdb)


@pl.api.register_series_namespace("geoip")
class GeoIpSeriesExt:
    """
    This class contains tools for parsing IP addresses.

    Polars Namespace: geoip

    Example: df["srcip"].geoip.asn()
    """

    # noqa: D102
    def __init__(self, s: pl.Series):
        self._s: pl.Series = s

    def asn(self, reload_mmdb: bool = False) -> pl.Series:
        return pl.select(asn(self._s, reload_mmdb=reload_mmdb)).to_series()

    def full(self, reload_mmdb: bool = False) -> pl.Series:
        return pl.select(full(self._s, reload_mmdb=reload_mmdb)).to_series()
