from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
from polars.plugins import register_plugin_function

if TYPE_CHECKING:
    from polars_iptools.typing import IntoExpr

LIB = Path(__file__).parent


__all__ = [
    "full",
]


def full(expr: IntoExpr, reload_mmdb: bool = False) -> pl.Expr:
    """
    Retrieve full Spur IP Context metadata of IPv4 and IPv6 addresses

    If you are customer of Spur, you can download a subset of their
    Anonymization and Anonymization+Residential feeds in Maxmind MMDB
    format. See https://docs.spur.us/feeds?id=feed-export-utility for
    more details.

    Parameters
    ----------
    expr
        The expression or column containing IP addresses.
    reload_mmdb : bool, optional
        Force reload/reinitialize of Spur's mmdb reader. Default is False.

    Returns
    -------
    pl.Expr
        An expression that returns a struct containing the following fields:
        - client_count : Float32
        - infrastructure : String
        - location_city : String
        - location_country : String
        - location_state : String
        - tag : String
        - services : List[String]

    Example
    -------
    >>> import polars as pl
    >>> import polars_iptools as ip

    >>> df = pl.DataFrame({"ip":["8.8.8.8", "192.168.1.1", "2606:4700::1111", "999.abc.def.123"]})
    >>> df = df.with_columns([ip.spur.full(pl.col("ip")).alias("spurcontext")])

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
            ('spurcontext',
             Struct({'client_count': Float32, 'infrastructure': String,
             'location_city': String, 'location_country': String,
             'location_state': String, 'tag': String, 'services': List(String)}))])
    Notes
    -----
    - IP addresses that are invalid or not found in the database will result in `null` values in the respective fields.
    """
    return register_plugin_function(
        args=[expr],
        plugin_path=LIB,
        function_name="pl_full_spur",
        kwargs={
            "reload_mmdb": reload_mmdb,
        },
        is_elementwise=True,
    )
