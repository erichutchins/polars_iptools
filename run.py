import polars as pl

import polars_iptools as ip

df = pl.DataFrame(
    {
        "ip": [
            "8.8.8.8/32",
            "128.143.2.7",
            "1.1.1.10",
            "10.0.0.3",
            "fc00:0000:0000:0000:0000:dead:beef:0000",
            "192.168.3.3",
            "asdadasada",
        ],
    }
)
result = df.with_columns(
    [
        ip.is_valid("ip").alias("valid"),
        ip.is_private("ip").alias("priv"),
        ip.ipv4_to_numeric(pl.col("ip")).alias("numeric"),
        ip.is_in(pl.col("ip"), ["1.1.1.10/16"]).alias("subnet"),
        ip.geoip.asn(pl.col("ip")).alias("ipasn"),
        ip.geoip.full(pl.col("ip")).alias("ipfull"),
    ]
)
print(result)
print(result.write_ndjson())
