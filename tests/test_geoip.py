from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import polars_iptools as ip


@pytest.fixture(scope="session")
def maxmind_dir():
    # Get the current test directory
    test_dir = Path(__file__).parent.resolve()
    return test_dir / "maxmind"


@pytest.fixture(scope="function", autouse=True)
def set_global_env_vars(monkeypatch, maxmind_dir):
    # Set the environment variables
    monkeypatch.setenv("MAXMIND_MMDB_DIR", str(maxmind_dir))


def test_asn_lookup():
    """
    In the test maxmind db, 67.43.156.1 has just an AS number but no Org name
    Invalid ips return an empty string
    """
    ips = ["67.43.156.1", "240b::beef:0:24", "999.9.9.9"]

    df = pl.DataFrame({"ip": ips})
    result = df.with_columns(result=ip.geoip.asn("ip"))

    expected_df = pl.DataFrame(
        {
            "ip": ips,
            "result": ["AS35908", "AS2516 KDDI KDDI CORPORATION", ""],
        }
    )

    assert_frame_equal(result, expected_df)


def test_full_geoip_lookup():
    schema = {
        "ip": pl.Utf8,
        "result": pl.Struct(
            [
                pl.Field("asnnum", pl.UInt32),
                pl.Field("asnorg", pl.Utf8),
                pl.Field("city", pl.Utf8),
                pl.Field("continent", pl.Utf8),
                pl.Field("subdivision_iso", pl.Utf8),
                pl.Field("subdivision", pl.Utf8),
                pl.Field("country_iso", pl.Utf8),
                pl.Field("country", pl.Utf8),
                pl.Field("latitude", pl.Float64),
                pl.Field("longitude", pl.Float64),
                pl.Field("timezone", pl.Utf8),
            ]
        ),
    }

    ips = ["67.43.156.1", "240b::beef:0:24"]

    df = pl.DataFrame({"ip": ips})
    result = df.with_columns(result=ip.geoip.full("ip"))

    expected_df = pl.DataFrame(
        {
            "ip": ips,
            "result": [
                (
                    35908,
                    "",
                    "",
                    "AS",
                    "",
                    "",
                    "BT",
                    "Bhutan",
                    27.5,
                    90.5,
                    "Asia/Thimphu",
                ),
                (2516, "KDDI KDDI CORPORATION", "", "", "", "", "", "", 0.0, 0.0, ""),
            ],
        },
        schema_overrides=schema,
    )

    assert_frame_equal(result, expected_df)
