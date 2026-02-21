import polars as pl

import polars_iptools.geoip as geoip  # noqa: F401
import polars_iptools.spur as spur  # noqa: F401
from polars_iptools._internal import __version__ as __version__
from polars_iptools.iptools import *  # noqa: F403
from polars_iptools.types import IP_DTYPES, IPAddress, IPv4

# Register extension types with Polars.
# This is the sole registration point — the Rust side does not register,
# avoiding double-registration issues with Polars' extension type registry.
pl.register_extension_type("polars_iptools.ipv4", IPv4)
pl.register_extension_type("polars_iptools.ip_address", IPAddress)

__all__ = [
    "IPv4",
    "IPAddress",
    "IP_DTYPES",
    "geoip",
    "spur",
    "__version__",
]
