import polars as pl
import pytest
from polars.testing import assert_frame_equal

import polars_iptools as ip


def test_is_valid():
    """
    is_valid supports ipv4 and ipv6
    """
    ips = ["8.8.8.8", "1.1.1.1", "999.9.9.9", "2606:4700::1111"]

    df = pl.DataFrame({"ip": ips})
    result = df.with_columns(result=ip.is_valid("ip"))

    expected_df = pl.DataFrame(
        {
            "ip": ips,
            "result": [True, True, False, True],
        }
    )

    assert_frame_equal(result, expected_df)


def test_is_private():
    """
    is_private only supports ipv4
    """
    ips = [
        "8.8.8.8",  # public - google dns
        "192.168.30.30",  # private - 192.168/16
        "10.1.2.3",  # private - 10/8
        "172.16.25.30",  # private - 172.16/12
        "999.9.9.9",  # invalid ipv4
        "2606:4700::1111",  # public - cloudflare ipv6 dns
        "fd12:3456:789a:1::1",  # private but no support for ipv6 addr
    ]

    df = pl.DataFrame({"ip": ips})
    result = df.with_columns(result=ip.is_private("ip"))

    expected_df = pl.DataFrame(
        {
            "ip": ips,
            "result": [False, True, True, True, False, False, False],
        }
    )

    assert_frame_equal(result, expected_df)


def test_ipv4_to_numeric():
    """
    to_numeric only supports ipv4
    """
    ips = [
        "8.8.8.8",  # public - google dns
        "192.168.30.30",  # private - 192.168/16
        "999.9.9.9",  # invalid ipv4
        "2606:4700::1111",  # public - cloudflare ipv6 dns
    ]

    df = pl.DataFrame({"ip": ips})
    result = df.with_columns(result=ip.ipv4_to_numeric("ip"))

    expected_df = pl.DataFrame(
        {
            "ip": ips,
            "result": [134744072, 3232243230, None, None],
        },
        strict=False,
    ).with_columns(pl.col("result").cast(pl.UInt32))

    assert_frame_equal(result, expected_df)


def test_numeric_to_ipv4():
    """
    numeric only supports ipv4
    """
    ips = [
        134744072,  # 8.8.8.8
        3232243230,  # 192.168.30.30
        4294967295,  # 255.255.255.255
        4294967296,  # 255.255.255.255 + 1 (invalid u32)
    ]

    df = pl.DataFrame({"ip": ips})
    result = df.with_columns(result=ip.numeric_to_ipv4("ip"))

    expected_df = pl.DataFrame(
        {
            "ip": ips,
            "result": ["8.8.8.8", "192.168.30.30", "255.255.255.255", None],
        },
        strict=False,
    )

    assert_frame_equal(result, expected_df)


def test_numeric_to_ipv4_strings():
    """
    numeric returns NA for string input
    """
    ips = ["abcde"]

    df = pl.DataFrame({"ip": ips})
    result = df.with_columns(result=ip.numeric_to_ipv4("ip"))

    expected_df = pl.DataFrame(
        {
            "ip": ips,
            "result": [None],
        },
    ).with_columns(pl.col("result").cast(pl.String))

    assert_frame_equal(result, expected_df)


def test_is_in_list():
    ips = ["8.8.8.8", "1.1.1.1", "abcd", "2606:4700::1111"]
    networks = ["8.8.8.0/24", "2606:4700::/32"]

    df = pl.DataFrame({"ip": ips})
    result = df.with_columns(result=ip.is_in("ip", networks))

    expected_df = pl.DataFrame(
        {
            "ip": ips,
            "result": [True, False, None, True],
        },
    )

    assert_frame_equal(result, expected_df)


def test_is_in_series():
    ips = ["8.8.8.8", "1.1.1.1", "abcd", "2606:4700::1111"]
    networks = pl.Series(["8.8.8.0/24", "2606:4700::/32"])

    df = pl.DataFrame({"ip": ips})
    result = df.with_columns(result=ip.is_in("ip", networks))

    expected_df = pl.DataFrame(
        {
            "ip": ips,
            "result": [True, False, None, True],
        },
    )

    assert_frame_equal(result, expected_df)


def test_is_in_set():
    ips = ["8.8.8.8", "1.1.1.1", "abcd", "2606:4700::1111"]
    networks = set(["8.8.8.0/24", "2606:4700::/32"])

    df = pl.DataFrame({"ip": ips})
    result = df.with_columns(result=ip.is_in("ip", networks))

    expected_df = pl.DataFrame(
        {
            "ip": ips,
            "result": [True, False, None, True],
        },
    )

    assert_frame_equal(result, expected_df)


def test_is_in_invalid_network():
    ips = ["8.8.8.8", "1.1.1.1", "2606:4700::1111"]
    networks = set(["8.8.8.0/55"])

    df = pl.DataFrame({"ip": ips})

    with pytest.raises(pl.exceptions.ComputeError, match="Invalid CIDR range"):
        _ = df.with_columns(result=ip.is_in("ip", networks))


def test_extract_ipv4():
    """
    test extracting ipv4-like strings
    """
    text = [
        "255.255.255.255",
        '{"json":"8.8.8.8"}',
        "X-Forwarded-For: 203.0.113.195, 70.41.3.18, 150.172.238.178",
        "X-Forwarded-For: 203.0.113.195:41237, 198.51.100.100:38523",
    ]

    df = pl.DataFrame({"text": text})
    result = df.with_columns(result=ip.extract_all_ips("text"))

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


def test_extract_ipv4_and_ipv6():
    """
    this is the same test as before but with ipv6 regex enabled.
    should still get the same results
    """
    text = [
        "255.255.255.255",
        '{"json":"8.8.8.8"}',
        "X-Forwarded-For: 203.0.113.195, 70.41.3.18, 150.172.238.178",
        "X-Forwarded-For: 203.0.113.195:41237, 198.51.100.100:38523",
    ]

    df = pl.DataFrame({"text": text})
    result = df.with_columns(result=ip.extract_all_ips("text", ipv6=True))

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


def test_extract_ipv4_and_ipv6_with_ipv6():
    """
    now actually with ipv6
    """
    # XFF examples from https://en.wikipedia.org/wiki/X-Forwarded-For
    text = [
        "::1",
        '{"json":"8.8.8.8"}',
        "X-Forwarded-For: [2001:db8::1a2b:3c4d]:41237, 198.51.100.100:26321",
        "X-Forwarded-For: 2001:db8:85a3:8d3:1319:8a2e:370:7348",
        'Forwarded: for="[2001:db8::1234]"',
    ]

    df = pl.DataFrame({"text": text})
    result = df.with_columns(result=ip.extract_all_ips("text", ipv6=True))

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
