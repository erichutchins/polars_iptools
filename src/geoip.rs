#![allow(clippy::unused_unit)]
use maxminddb::geoip2;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use std::fmt::Write;
use std::net::IpAddr;

use crate::maxmind::{MaxMindDB, MAXMIND_FIELDS};
use crate::utils::{create_builders, MMDBKwargs};

// borrowing pattern from github.com/abstractqqq/polars_istr
fn geoip_full_output(_: &[Field]) -> PolarsResult<Field> {
    let v: Vec<Field> = MAXMIND_FIELDS
        .iter()
        .map(|(name, data_type)| Field::new(name, data_type.clone()))
        .collect();

    Ok(Field::new("", DataType::Struct(v)))
}

// Build struct containing ASN and City level metadata of input IP addresses
#[polars_expr(output_type_func=geoip_full_output)]
fn pl_full_geoip(inputs: &[Series], kwargs: MMDBKwargs) -> PolarsResult<Series> {
    if kwargs.reload_mmdb {
        MaxMindDB::reload()?;
    }

    let binding = MaxMindDB::get_or_init()?;
    let mdb = binding
            .as_ref()
            .ok_or_else(|| PolarsError::ComputeError("Error: MaxMindDB is not initialized. Please ensure that the MMDB files are correctly placed and accessible.".into()))?
            .as_ref()
            .map_err(|e| {
                PolarsError::ComputeError(format!("Failed to initialize MaxMindDB: {}", e).into())
            })?;

    let ca: &StringChunked = inputs[0].str()?;

    let mut builders = create_builders(&MAXMIND_FIELDS, ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(ip_s) = op_s {
            if let Ok(ip) = ip_s.parse::<IpAddr>() {
                let geoipresult = mdb.iplookup(ip);

                // add values to the builders
                // Important: these must be in same order as MAXMIND_FIELDS
                // sort alphabetically to ensure
                builders[0].append_value(geoipresult.asnnum);
                builders[1].append_value(geoipresult.asnorg);
                builders[2].append_value(geoipresult.city);
                builders[3].append_value(geoipresult.continent);
                builders[4].append_value(geoipresult.subdivision_iso);
                builders[5].append_value(geoipresult.subdivision);
                builders[6].append_value(geoipresult.country_iso);
                builders[7].append_value(geoipresult.country);
                builders[8].append_value(geoipresult.latitude);
                builders[9].append_value(geoipresult.longitude);
                builders[10].append_value(geoipresult.timezone);
                builders[11].append_value(geoipresult.postalcode);
            } else {
                // invalid ip, so append nulls for everything
                builders
                    .iter_mut()
                    .for_each(|builder| builder.append_null());
            }
        } else {
            // null input, so append nulls for everything
            builders
                .iter_mut()
                .for_each(|builder| builder.append_null());
        }
    });

    let series: Vec<Series> = builders.into_iter().map(|b| b.finish()).collect();
    StructChunked::from_series("geoip", &series).map(|ca| ca.into_series())
}

// Get ASN and org name for Internet routed IP addresses
#[polars_expr(output_type=String)]
fn pl_get_asn(inputs: &[Series], kwargs: MMDBKwargs) -> PolarsResult<Series> {
    if kwargs.reload_mmdb {
        MaxMindDB::reload()?;
    }

    let binding = MaxMindDB::get_or_init()?;
    let mdb = binding
        .as_ref()
        .ok_or_else(|| PolarsError::ComputeError("MaxMindDB is not initialized".into()))?
        .as_ref()
        .map_err(|_| {
            PolarsError::ComputeError("Failed to initialize MaxMindDB in map_err closure".into())
        })?;

    let asn_reader = mdb.asn_reader();

    let ca: &StringChunked = inputs[0].str()?;

    let out: StringChunked = ca.apply_into_string_amortized(|value: &str, output: &mut String| {
        if let Ok(ip) = value.parse::<IpAddr>() {
            // only emit ASN information if we have a) a valid IP and b) it exists
            // in the asn mmdb. if it's a valid ip but not in the mmdb (e.g. private IPs),
            // still leave the output blank
            if let Ok(asnrecord) = asn_reader.lookup::<geoip2::Asn>(ip) {
                let asnnum = asnrecord.autonomous_system_number.unwrap_or(0);
                let asnorg = asnrecord.autonomous_system_organization.unwrap_or("");
                if asnorg.is_empty() {
                    write!(output, "AS{}", asnnum).unwrap()
                } else {
                    write!(output, "AS{} {}", asnnum, asnorg).unwrap()
                }
            }
        }
    });

    Ok(out.into_series())
}
