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
    schema = pl.Schema(
        {
            "ip": pl.String,
            "result": pl.Struct(
                {
                    "asnnum": pl.UInt32,
                    "asnorg": pl.String,
                    "city": pl.String,
                    "continent": pl.String,
                    "subdivision_iso": pl.String,
                    "subdivision": pl.String,
                    "country_iso": pl.String,
                    "country": pl.String,
                    "latitude": pl.Float64,
                    "longitude": pl.Float64,
                    "timezone": pl.String,
                }
            ),
        }
    )

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
