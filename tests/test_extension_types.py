import warnings

import polars as pl
from polars.testing import assert_frame_equal

import polars_iptools as ip
from polars_iptools.types import IP_DTYPES, IPAddress, IPv4

# -- to_ipv4 round-trip ------------------------------------------------


def test_to_ipv4_roundtrip():
    """str -> IPv4 -> str round-trip preserves values."""
    ips = ["8.8.8.8", "192.168.1.1", "255.255.255.255"]
    df = pl.DataFrame({"ip": ips})

    result = df.select(
        ip.to_string(pl.col("ip").ip.to_ipv4().ext.storage()).alias("result")
    )

    expected = pl.DataFrame({"result": ips})
    assert_frame_equal(result, expected)


def test_to_ipv4_invalid_returns_null():
    """Invalid IPv4 strings produce nulls.

    Note: Polars panics when wrapping all-null results into extension types
    (polars-expr/dispatch/extension.rs:22). We verify the underlying plugin
    returns nulls correctly, and that mixed valid/invalid input works.

    Upstream: https://github.com/pola-rs/polars/issues/25322
    """
    df = pl.DataFrame({"ip": ["8.8.8.8", "999.9.9.9", "not_an_ip"]})

    result = df.select(ip.to_ipv4("ip").ip.to_string().alias("result"))

    assert result["result"][0] == "8.8.8.8"
    assert result["result"][1] is None
    assert result["result"][2] is None


def test_to_ipv4_null_passthrough():
    """Null inputs pass through as null."""
    df = pl.DataFrame({"ip": [None, "8.8.8.8", None]}, schema={"ip": pl.String})

    result = df.select(ip.to_ipv4("ip").ext.storage().alias("result"))

    assert result["result"].null_count() == 2
    assert result["result"][1] is not None


# -- to_address round-trip ---------------------------------------------


def test_to_address_ipv4_roundtrip():
    """IPv4 str -> IPAddress -> str round-trip."""
    ips = ["8.8.8.8", "1.1.1.1"]
    df = pl.DataFrame({"ip": ips})

    result = df.select(
        ip.to_string(pl.col("ip").ip.to_address().ext.storage()).alias("result")
    )

    expected = pl.DataFrame({"result": ips})
    assert_frame_equal(result, expected)


def test_to_address_ipv6_roundtrip():
    """IPv6 str -> IPAddress -> str round-trip."""
    ips = ["2606:4700::1111", "::1"]
    df = pl.DataFrame({"ip": ips})

    result = df.select(
        ip.to_string(pl.col("ip").ip.to_address().ext.storage()).alias("result")
    )

    expected = pl.DataFrame({"result": ips})
    assert_frame_equal(result, expected)


def test_to_address_mixed():
    """Mixed IPv4/IPv6 -> IPAddress -> str round-trip."""
    ips = ["8.8.8.8", "2606:4700::1111", "192.168.1.1", "::1"]
    df = pl.DataFrame({"ip": ips})

    result = df.select(
        ip.to_string(pl.col("ip").ip.to_address().ext.storage()).alias("result")
    )

    expected = pl.DataFrame({"result": ips})
    assert_frame_equal(result, expected)


def test_to_address_invalid_returns_null():
    """Invalid IPs produce nulls in IPAddress.

    Note: Polars panics when wrapping all-null results into extension types,
    so we include at least one valid IP to avoid the edge case.

    Upstream: https://github.com/pola-rs/polars/issues/25322
    """
    df = pl.DataFrame({"ip": ["8.8.8.8", "not_an_ip", "999.9.9.9"]})

    result = df.select(ip.to_address("ip").ip.to_string().alias("result"))

    assert result["result"][0] == "8.8.8.8"
    assert result["result"][1] is None
    assert result["result"][2] is None


# -- IP_DTYPES ---------------------------------------------------------


def test_ip_dtypes_contains_ipv4():
    assert IPv4() in IP_DTYPES


def test_ip_dtypes_contains_ipaddress():
    assert IPAddress() in IP_DTYPES


def test_ip_dtypes_length():
    assert len(IP_DTYPES) == 2


# -- Namespace .ip.to_string ------------------------------------------


def test_expr_namespace_to_string():
    """IpExprExt.to_string works on IPv4 extension columns."""
    df = pl.DataFrame({"ip": ["8.8.8.8", "1.1.1.1"]})

    result = df.select(pl.col("ip").ip.to_ipv4().ip.to_string().alias("result"))

    expected = pl.DataFrame({"result": ["8.8.8.8", "1.1.1.1"]})
    assert_frame_equal(result, expected)


def test_expr_namespace_to_string_ipaddress():
    """IpExprExt.to_string works on IPAddress extension columns."""
    df = pl.DataFrame({"ip": ["8.8.8.8", "2606:4700::1111"]})

    result = df.select(pl.col("ip").ip.to_address().ip.to_string().alias("result"))

    expected = pl.DataFrame({"result": ["8.8.8.8", "2606:4700::1111"]})
    assert_frame_equal(result, expected)


def test_series_namespace_to_string():
    """IpSeriesExt.to_string works on IPv4 extension Series."""
    s = pl.Series("ip", ["8.8.8.8", "1.1.1.1"])
    ipv4_series = pl.select(ip.to_ipv4(s)).to_series()

    result = ipv4_series.ip.to_string()

    expected = pl.Series("ip_string", ["8.8.8.8", "1.1.1.1"])
    assert result.to_list() == expected.to_list()


# -- Deprecation warnings ----------------------------------------------


def test_expr_ipv4_to_numeric_deprecation():
    """IpExprExt.ipv4_to_numeric() emits DeprecationWarning."""
    df = pl.DataFrame({"ip": ["8.8.8.8"]})

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _ = df.select(pl.col("ip").ip.ipv4_to_numeric())
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "deprecated" in str(w[0].message).lower()


def test_expr_numeric_to_ipv4_deprecation():
    """IpExprExt.numeric_to_ipv4() emits DeprecationWarning."""
    df = pl.DataFrame({"ip": [134744072]})

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _ = df.select(pl.col("ip").ip.numeric_to_ipv4())
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)


def test_series_ipv4_to_numeric_deprecation():
    """IpSeriesExt.ipv4_to_numeric() emits DeprecationWarning."""
    s = pl.Series("ip", ["8.8.8.8"])

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _ = s.ip.ipv4_to_numeric()
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)


def test_series_numeric_to_ipv4_deprecation():
    """IpSeriesExt.numeric_to_ipv4() emits DeprecationWarning."""
    s = pl.Series("ip", [134744072], dtype=pl.UInt32)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        result = s.ip.numeric_to_ipv4()
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert result[0] == "8.8.8.8"


# -- Deprecated methods still produce correct results -------------------


def test_deprecated_ipv4_to_numeric_correctness():
    """Deprecated .ip.ipv4_to_numeric() still returns correct values."""
    df = pl.DataFrame({"ip": ["8.8.8.8", "192.168.30.30"]})

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        result = df.select(pl.col("ip").ip.ipv4_to_numeric().alias("result"))

    expected = pl.DataFrame(
        {"result": [134744072, 3232243230]},
    ).with_columns(pl.col("result").cast(pl.UInt32))

    assert_frame_equal(result, expected)


def test_deprecated_numeric_to_ipv4_correctness():
    """Deprecated .ip.numeric_to_ipv4() still returns correct values."""
    df = pl.DataFrame({"ip": [134744072, 3232243230]})

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        result = df.select(pl.col("ip").ip.numeric_to_ipv4().alias("result"))

    expected = pl.DataFrame({"result": ["8.8.8.8", "192.168.30.30"]})
    assert_frame_equal(result, expected)


# -- Parquet / IPC round-trip ---------------------------------------------


def test_parquet_roundtrip_ipv4(tmp_path):
    """IPv4 extension type survives Parquet write/read."""
    df = pl.DataFrame({"ip": ["8.8.8.8", "1.1.1.1", "192.168.1.1"]}).select(
        ip.to_ipv4("ip")
    )
    path = tmp_path / "ipv4.parquet"
    df.write_parquet(path)
    result = pl.read_parquet(path)

    assert result.dtypes == df.dtypes
    assert result.shape == df.shape
    # values survive round-trip
    assert_frame_equal(
        result.select(pl.col("ip").ip.to_string()),
        df.select(pl.col("ip").ip.to_string()),
    )


def test_parquet_roundtrip_ipaddress(tmp_path):
    """IPAddress extension type survives Parquet write/read."""
    df = pl.DataFrame({"ip": ["8.8.8.8", "2606:4700::1111", "::1"]}).select(
        ip.to_address("ip")
    )
    path = tmp_path / "ipaddr.parquet"
    df.write_parquet(path)
    result = pl.read_parquet(path)

    assert result.dtypes == df.dtypes
    assert result.shape == df.shape
    assert_frame_equal(
        result.select(pl.col("ip").ip.to_string()),
        df.select(pl.col("ip").ip.to_string()),
    )


def test_ipc_roundtrip_ipv4(tmp_path):
    """IPv4 extension type survives IPC write/read."""
    df = pl.DataFrame({"ip": ["8.8.8.8", "1.1.1.1"]}).select(ip.to_ipv4("ip"))
    path = tmp_path / "ipv4.ipc"
    df.write_ipc(path)
    result = pl.read_ipc(path)

    assert result.dtypes == df.dtypes
    assert_frame_equal(
        result.select(pl.col("ip").ip.to_string()),
        df.select(pl.col("ip").ip.to_string()),
    )


def test_ipc_roundtrip_ipaddress(tmp_path):
    """IPAddress extension type survives IPC write/read."""
    df = pl.DataFrame({"ip": ["8.8.8.8", "2606:4700::1111"]}).select(
        ip.to_address("ip")
    )
    path = tmp_path / "ipaddr.ipc"
    df.write_ipc(path)
    result = pl.read_ipc(path)

    assert result.dtypes == df.dtypes
    assert_frame_equal(
        result.select(pl.col("ip").ip.to_string()),
        df.select(pl.col("ip").ip.to_string()),
    )
