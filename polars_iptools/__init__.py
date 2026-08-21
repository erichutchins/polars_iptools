import polars as pl

from polars_iptools import (
    geoip,
    spur,
)
from polars_iptools._internal import __version__ as __version__
from polars_iptools.iptools import *
from polars_iptools.types import IP_DTYPES, IPAddress, IPv4

# Register extension types with Polars.
# This is the sole registration point — the Rust side does not register,
# avoiding double-registration issues with Polars' extension type registry.
pl.register_extension_type("polars_iptools.ipv4", IPv4)
pl.register_extension_type("polars_iptools.ip_address", IPAddress)

__all__ = [
    "IP_DTYPES",
    "IPAddress",
    "IPv4",
    "__version__",
    "geoip",
    "spur",
]
