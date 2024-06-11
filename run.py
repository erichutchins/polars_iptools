import polars as pl
from polars_geoip import lookup_all

df = pl.DataFrame({
    'ip': ['8.8.8.8', '128.143.2.7', '1.1.1.1'],
})
result = df.with_columns(geoip = lookup_all('ip'))
print(result)
print(result.write_ndjson())
