import polars as pl
import pytest
from polars.testing import assert_frame_equal

import polars_iptools as ip


@pytest.fixture(
    params=[
        ("DataFrame", lambda data, expr: pl.DataFrame(data).with_columns(result=expr)),
        (
            "LazyFrame",
            lambda data, expr: pl.LazyFrame(data).with_columns(result=expr).collect(),
        ),
    ]
)
def frame_factory(request):
    """
    Fixture that creates and processes both DataFrame and LazyFrame variants.

    Returns a factory function that:
    - Creates a DataFrame or LazyFrame from the input data
    - Applies the given expression to create a "result" column
    - Returns the resulting frame (collecting LazyFrames)
    """
    name, factory = request.param
    return name, factory


def test_is_valid(frame_factory):
    """is_valid supports ipv4 and ipv6"""
    _, factory = frame_factory
    ips = ["8.8.8.8", "1.1.1.1", "999.9.9.9", "2606:4700::1111"]

    result = factory({"ip": ips}, ip.is_valid("ip"))

    expected_df = pl.DataFrame(
        {
            "ip": ips,
            "result": [True, True, False, True],
        }
    )

    assert_frame_equal(result, expected_df)


def test_is_private(frame_factory):
    """is_private only supports ipv4"""
    _, factory = frame_factory
    ips = [
        "8.8.8.8",  # public - google dns
        "192.168.30.30",  # private - 192.168/16
        "10.1.2.3",  # private - 10/8
        "172.16.25.30",  # private - 172.16/12
        "999.9.9.9",  # invalid ipv4
        "2606:4700::1111",  # public - cloudflare ipv6 dns
        "fd12:3456:789a:1::1",  # private but no support for ipv6 addr
    ]

    result = factory({"ip": ips}, ip.is_private("ip"))

    expected_df = pl.DataFrame(
        {
            "ip": ips,
            "result": [False, True, True, True, False, False, False],
        }
    )

    assert_frame_equal(result, expected_df)


def test_ipv4_to_numeric(frame_factory):
    """to_numeric only supports ipv4"""
    _, factory = frame_factory
    ips = [
        "8.8.8.8",  # public - google dns
        "192.168.30.30",  # private - 192.168/16
        "999.9.9.9",  # invalid ipv4
        "2606:4700::1111",  # public - cloudflare ipv6 dns
    ]

    result = factory({"ip": ips}, ip.ipv4_to_numeric("ip"))

    expected_df = pl.DataFrame(
        {
            "ip": ips,
            "result": [134744072, 3232243230, None, None],
        },
        strict=False,
    ).with_columns(pl.col("result").cast(pl.UInt32))

    assert_frame_equal(result, expected_df)


def test_numeric_to_ipv4(frame_factory):
    """numeric only supports ipv4"""
    _, factory = frame_factory
    ips = [
        134744072,  # 8.8.8.8
        3232243230,  # 192.168.30.30
        4294967295,  # 255.255.255.255
        4294967296,  # 255.255.255.255 + 1 (invalid u32)
    ]

    result = factory({"ip": ips}, ip.numeric_to_ipv4("ip"))

    expected_df = pl.DataFrame(
        {
            "ip": ips,
            "result": ["8.8.8.8", "192.168.30.30", "255.255.255.255", None],
        },
        strict=False,
    )

    assert_frame_equal(result, expected_df)


def test_numeric_to_ipv4_strings(frame_factory):
    """numeric returns NA for string input"""
    _, factory = frame_factory
    ips = ["abcde"]

    result = factory({"ip": ips}, ip.numeric_to_ipv4("ip"))

    expected_df = pl.DataFrame(
        {
            "ip": ips,
            "result": [None],
        }
    ).with_columns(pl.col("result").cast(pl.String))

    assert_frame_equal(result, expected_df)


@pytest.mark.parametrize(
    "networks",
    [
        ["8.8.8.0/24", "2606:4700::/32"],
        pl.Series(["8.8.8.0/24", "2606:4700::/32"]),
        set(["8.8.8.0/24", "2606:4700::/32"]),
    ],
)
def test_is_in(frame_factory, networks):
    _, factory = frame_factory
    ips = ["8.8.8.8", "1.1.1.1", "abcd", "2606:4700::1111"]

    result = factory({"ip": ips}, ip.is_in("ip", networks))

    expected_df = pl.DataFrame(
        {
            "ip": ips,
            "result": [True, False, None, True],
        }
    )

    assert_frame_equal(result, expected_df)


def test_is_in_invalid_network(frame_factory):
    name, factory_fn = frame_factory
    ips = ["8.8.8.8", "1.1.1.1", "2606:4700::1111"]
    networks = set(["8.8.8.0/55"])

    # For this test, we need to handle the expected exception manually
    with pytest.raises(pl.exceptions.ComputeError, match="Invalid CIDR range"):
        if name == "DataFrame":
            df = pl.DataFrame({"ip": ips})
            _ = df.with_columns(result=ip.is_in("ip", networks))
        else:  # LazyFrame
            df = pl.LazyFrame({"ip": ips})
            _ = df.with_columns(result=ip.is_in("ip", networks)).collect()


def test_extract_ipv4(frame_factory):
    """test extracting ipv4-like strings"""
    _, factory = frame_factory
    text = [
        "255.255.255.255",
        '{"json":"8.8.8.8"}',
        "X-Forwarded-For: 203.0.113.195, 70.41.3.18, 150.172.238.178",
        "X-Forwarded-For: 203.0.113.195:41237, 198.51.100.100:38523",
    ]

    result = factory({"text": text}, ip.extract_all_ips("text"))

    expected_df = pl.DataFrame(
        {
            "text": text,
            "result": [
                ["255.255.255.255"],
                ["8.8.8.8"],
                ["203.0.113.195", "70.41.3.18", "150.172.238.178"],
                ["203.0.113.195", "198.51.100.100"],
            ],
        }
    )

    assert_frame_equal(result, expected_df)


def test_extract_ipv4_and_ipv6(frame_factory):
    """test with ipv4 and ipv6 regex enabled"""
    _, factory = frame_factory
    text = [
        "255.255.255.255",
        '{"json":"8.8.8.8"}',
        "X-Forwarded-For: 203.0.113.195, 70.41.3.18, 150.172.238.178",
        "X-Forwarded-For: 203.0.113.195:41237, 198.51.100.100:38523",
    ]

    result = factory({"text": text}, ip.extract_all_ips("text", ipv6=True))

    expected_df = pl.DataFrame(
        {
            "text": text,
            "result": [
                ["255.255.255.255"],
                ["8.8.8.8"],
                ["203.0.113.195", "70.41.3.18", "150.172.238.178"],
                ["203.0.113.195", "198.51.100.100"],
            ],
        }
    )

    assert_frame_equal(result, expected_df)


def test_extract_ipv4_and_ipv6_with_ipv6(frame_factory):
    """test with actual ipv6 addresses"""
    _, factory = frame_factory
    # XFF examples from https://en.wikipedia.org/wiki/X-Forwarded-For
    text = [
        "::1",
        '{"json":"8.8.8.8"}',
        "X-Forwarded-For: [2001:db8::1a2b:3c4d]:41237, 198.51.100.100:26321",
        "X-Forwarded-For: 2001:db8:85a3:8d3:1319:8a2e:370:7348",
        'Forwarded: for="[2001:db8::1234]"',
    ]

    result = factory({"text": text}, ip.extract_all_ips("text", ipv6=True))

    expected_df = pl.DataFrame(
        {
            "text": text,
            "result": [
                ["::1"],
                ["8.8.8.8"],
                ["2001:db8::1a2b:3c4d", "198.51.100.100"],
                ["2001:db8:85a3:8d3:1319:8a2e:370:7348"],
                ["2001:db8::1234"],
            ],
        }
    )

    assert_frame_equal(result, expected_df)
