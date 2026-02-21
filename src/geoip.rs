#![allow(clippy::unused_unit)]
use maxminddb::geoip2;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use std::fmt::Write;

use crate::iptools::{series_to_ipaddrs, IpEntry};
use crate::maxmind::{MaxMindDB, MaxmindBuilders};
use crate::utils::MMDBKwargs;

// borrowing pattern from github.com/abstractqqq/polars_istr
fn geoip_full_output(_: &[Field]) -> PolarsResult<Field> {
    Ok(Field::new(
        PlSmallStr::EMPTY,
        DataType::Struct(MaxmindBuilders::fields()),
    ))
}

// Build struct containing ASN and City level metadata of input IP addresses.
// Accepts String, IPv4 (UInt32 storage), or IPAddress (Binary storage) columns.
#[polars_expr(output_type_func=geoip_full_output)]
fn pl_full_geoip(inputs: &[Series], kwargs: MMDBKwargs) -> PolarsResult<Series> {
    if kwargs.reload_mmdb {
        MaxMindDB::reload()?;
    }

    let guard = MaxMindDB::get_or_init()?;
    let mdb = guard.as_ref().map_err(|e| {
        PolarsError::ComputeError(format!("Failed to initialize MaxMindDB: {e}").into())
    })?;

    let ips = series_to_ipaddrs(&inputs[0])?;
    let mut builders = MaxmindBuilders::new(ips.len());

    for entry in &ips {
        match entry {
            IpEntry::Addr(ip) => {
                let geoipresult = mdb.iplookup(*ip);
                builders.append(&geoipresult);
            },
            IpEntry::Null | IpEntry::Invalid => {
                builders.append_null();
            },
        }
    }

    let series: Vec<Series> = builders.finish();
    StructChunked::from_series(PlSmallStr::from_static("geoip"), ips.len(), series.iter())
        .map(|ca| ca.into_series())
}

// Get ASN and org name for Internet routed IP addresses.
// Accepts String, IPv4 (UInt32 storage), or IPAddress (Binary storage) columns.
// Null inputs produce null output; invalid (non-null, unparseable) inputs produce "".
#[polars_expr(output_type=String)]
fn pl_get_asn(inputs: &[Series], kwargs: MMDBKwargs) -> PolarsResult<Series> {
    if kwargs.reload_mmdb {
        MaxMindDB::reload()?;
    }

    let guard = MaxMindDB::get_or_init()?;
    let mdb = guard.as_ref().map_err(|e| {
        PolarsError::ComputeError(format!("Failed to initialize MaxMindDB: {e}").into())
    })?;

    let asn_reader = mdb.asn_reader();
    let ips = series_to_ipaddrs(&inputs[0])?;

    let mut builder = StringChunkedBuilder::new(PlSmallStr::from_static("asn"), ips.len());

    for entry in &ips {
        match entry {
            IpEntry::Null => builder.append_null(),
            IpEntry::Invalid => builder.append_value(""),
            IpEntry::Addr(ip) => {
                let mut output = String::new();
                // only emit ASN information if we have a) a valid IP and b) it exists
                // in the asn mmdb. if it's a valid ip but not in the mmdb (e.g. private IPs),
                // still leave the output blank
                if let Some(asnrecord) = asn_reader
                    .lookup(*ip)
                    .ok()
                    .and_then(|lookup| lookup.decode::<geoip2::Asn>().ok().flatten())
                {
                    let asnnum = asnrecord.autonomous_system_number.unwrap_or(0);
                    let asnorg = asnrecord.autonomous_system_organization.unwrap_or("");
                    if asnorg.is_empty() {
                        let _ = write!(output, "AS{asnnum}");
                    } else {
                        let _ = write!(output, "AS{asnnum} {asnorg}");
                    }
                }
                builder.append_value(&output);
            },
        }
    }

    Ok(builder.finish().into_series())
}
