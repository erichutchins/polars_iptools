# Polars IPTools

Polars IPTools is a Rust-based extension to accelerates IP address manipulation and enrichment in Polars dataframes. This library includes various utility functions for working with IPv4 and IPv6 addresses and geoip enrichment using MaxMind databases.

## Install

```shell
pip install polars-iptools
```

## Examples

### Simple enrichments

IPTools' Rust implementation gives you speedy answers to basic IP questions like "is this a private IP?"

```python
>>> import polars as pl
>>> import polars_iptools as ip
>>> df = pl.DataFrame({'ip': ['8.8.8.8', '2606:4700::1111', '192.168.100.100', '172.21.1.1', '172.34.5.5', 'a.b.c.d']})
>>> df.with_columns(ip.is_private(pl.col('ip')).alias('is_private'))
shape: (6, 2)
┌─────────────────┬────────────┐
│ ip              ┆ is_private │
│ ---             ┆ ---        │
│ str             ┆ bool       │
╞═════════════════╪════════════╡
│ 8.8.8.8         ┆ false      │
│ 2606:4700::1111 ┆ false      │
│ 192.168.100.100 ┆ true       │
│ 172.21.1.1      ┆ true       │
│ 172.34.5.5      ┆ false      │
│ a.b.c.d         ┆ false      │
└─────────────────┴────────────┘
```

### `is_in` but for network ranges

Pandas and Polars have `is_in` functions to perform membership lookups. IPTools extends this to enable IP address membership in IP _networks_. This function works seamlessly with both IPv4 and IPv6 addresses and converts the specified networks into a [Level-Compressed trie (LC-Trie)](https://github.com/Orange-OpenSource/iptrie) for fast, efficient lookups.

```python
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
```

### GeoIP enrichment

Using [MaxMind's](https://www.maxmind.com/en/geoip-databases) _GeoLite2-ASN.mmdb_ and _GeoLite2-City.mmdb_ databases, IPTools provides offline enrichment of network ownership and geolocation.

`ip.geoip.full` returns a Polars struct containing all available metadata parameters. If you just want the ASN and AS organization, you can use `ip.geoip.asn`.

```python
>>> import polars as pl
>>> import polars_iptools as ip

>>> df = pl.DataFrame({"ip":["8.8.8.8", "192.168.1.1", "2606:4700::1111", "999.abc.def.123"]})
>>> df.with_columns([ip.geoip.full(pl.col("ip")).alias("geoip")])

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
```

## Environment Configuration

IPTools uses two MaxMind databases: _GeoLite2-ASN.mmdb_ and _GeoLite2-City.mmdb_. You only need these files if you call the geoip functions.

Set the `MAXMIND_MMDB_DIR` environment variable to tell the extension where these files are located.

```shell
export MAXMIND_MMDB_DIR=/path/to/your/mmdb/files
# or Windows users
set MAXMIND_MMDB_DIR=c:\path\to\your\mmdb\files
```

If the environment is not set, polars_iptools will check two other common locations (on Mac/Linux):

```
/usr/local/share/GeoIP
/opt/homebrew/var/GeoIP
```

## Credit

Developing this extension was super easy by following Marco Gorelli's [tutorial](https://marcogorelli.github.io/polars-plugins-tutorial/) and [cookiecutter template](https://github.com/MarcoGorelli/cookiecutter-polars-plugins).
