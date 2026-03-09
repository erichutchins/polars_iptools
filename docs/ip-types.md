# IP Extension Types

polars_iptools provides two Arrow extension types for storing IP addresses
efficiently and preserving type information through Parquet and IPC round-trips.

| Type | Storage | Best for |
|------|---------|----------|
| `IPv4` | `UInt32` (4 bytes) | IPv4-only datasets |
| `IPAddress` | `Binary` (16 bytes) | Mixed IPv4/IPv6, or any data written to Parquet/IPC |

Both types are registered automatically when you `import polars_iptools`.

## Reference

### `to_ipv4` — parse strings into the IPv4 type

```python
import polars as pl
import polars_iptools as ip

df = pl.DataFrame({"ip": ["8.8.8.8", "192.168.1.1", "invalid"]})
df.with_columns(ip.to_ipv4("ip"))
# shape: (3, 2)
# ┌─────────────┬─────────────┐
# │ ip          ┆ ip          │
# │ ---         ┆ ---         │
# │ str         ┆ ipv4        │
# ╞═════════════╪═════════════╡
# │ 8.8.8.8     ┆ 8.8.8.8     │
# │ 192.168.1.1 ┆ 192.168.1.1 │
# │ invalid     ┆ null        │
# └─────────────┴─────────────┘
```

### `to_address` — unified IPv4 + IPv6 type

```python
df = pl.DataFrame({"ip": ["8.8.8.8", "2606:4700::1111", "192.168.1.1"]})
df.with_columns(ip.to_address("ip"))
# shape: (3, 2)
# ┌─────────────────┬─────────────────┐
# │ ip              ┆ ip              │
# │ ---             ┆ ---             │
# │ str             ┆ ip_addr         │
# ╞═════════════════╪═════════════════╡
# │ 8.8.8.8         ┆ 8.8.8.8         │
# │ 2606:4700::1111 ┆ 2606:4700::1111 │
# │ 192.168.1.1     ┆ 192.168.1.1     │
# └─────────────────┴─────────────────┘
```

### `to_string` — convert back to canonical strings

```python
df = pl.DataFrame({"ip": ["8.8.8.8", "2606:4700::1111"]})
df.with_columns(ip.to_address("ip").ip.to_string())
```

### `.ip` namespace

All conversion functions are also available via the `.ip` expression and
Series namespace:

```python
# Expression namespace
pl.col("src_ip").ip.to_address()
pl.col("src_ip").ip.to_string()

# Series namespace
df["src_ip"].ip.to_ipv4()
```

### Extracting IPs from free text

```python
logs = pl.DataFrame({
    "text": [
        "conn from 8.8.8.8 and defanged 192[.]168[.]1[.]1",
        "public 1.1.1.1 and private 10.0.0.1",
    ]
})

# All IPs (defanged handled automatically)
logs.with_columns(ip.extract_ips("text"))

# Public only
logs.with_columns(ip.extract_public_ips("text"))

# Private only
logs.with_columns(ip.extract_private_ips("text"))

# With individual filters
logs.with_columns(
    ip.extract_ips("text", ipv6=True, ignore_loopback=True)
)
```

## End-to-end workflow

This example shows a realistic pipeline: parse raw log IPs into typed
columns, enrich with ASN data, write to Parquet, then reload — with the
`IPAddress` type preserved automatically.

```python
import polars as pl
import polars_iptools as ip

# --- 1. Raw log data with mixed IPv4/IPv6 ---
logs = pl.DataFrame({
    "ts": ["2024-01-01", "2024-01-01", "2024-01-01"],
    "src_ip": ["8.8.8.8", "2606:4700::1111", "192.168.1.1"],
    "bytes": [1024, 512, 256],
})

# --- 2. Parse to typed columns ---
# IPAddress handles both IPv4 and IPv6; preserves through Parquet/IPC
enriched = logs.with_columns(
    ip.to_address("src_ip").alias("src_ip_typed")
)

# --- 3. Enrich with ASN (works directly on IPAddress columns) ---
enriched = enriched.with_columns(
    ip.geoip.asn(pl.col("src_ip_typed")).alias("asn")
)
# shape: (3, 4)
# ┌────────────┬─────────────────┬───────┬──────────────────────┐
# │ ts         ┆ src_ip          ┆ bytes ┆ asn                  │
# │ ---        ┆ ---             ┆ ---   ┆ ---                  │
# │ str        ┆ str             ┆ i64   ┆ str                  │
# ╞════════════╪═════════════════╪═══════╪══════════════════════╡
# │ 2024-01-01 ┆ 8.8.8.8         ┆ 1024  ┆ AS15169 GOOGLE       │
# │ 2024-01-01 ┆ 2606:4700::1111 ┆ 512   ┆ AS13335 CLOUDFLARENET│
# │ 2024-01-01 ┆ 192.168.1.1     ┆ 256   ┆                      │
# └────────────┴─────────────────┴───────┴──────────────────────┘

# --- 4. Write to Parquet (extension type metadata is preserved) ---
enriched.write_parquet("logs_enriched.parquet")

# --- 5. Reload — src_ip_typed comes back as IPAddress, not Binary ---
reloaded = pl.read_parquet("logs_enriched.parquet")
print(reloaded.dtypes)
# [String, String, Int64, IPAddress, String]
#                          ^^^^^^^^^ type preserved!

# --- 6. Continue working with the typed column ---
reloaded.with_columns(
    pl.col("src_ip_typed").ip.to_string().alias("src_ip_str")
)
```

!!! tip "Why write typed columns to Parquet?"
    When you store raw IP strings, every read requires re-parsing. With
    `IPAddress`, the 16-byte binary is stored directly and the extension type
    name (`polars_iptools.ip_address`) is embedded in Parquet metadata — so
    re-reading restores the typed column automatically, with no extra
    `.with_columns()` needed.
