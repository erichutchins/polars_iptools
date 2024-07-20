#![allow(clippy::unused_unit)]
use maxminddb::geoip2;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use std::fmt::Write;
use std::net::IpAddr;

use crate::maxmind::{GeoIPKwargs, MaxMindDB};

// borrowing pattern from github.com/abstractqqq/polars_istr
fn geoip_full_output(_: &[Field]) -> PolarsResult<Field> {
    let asnnum = Field::new("asnnum", DataType::UInt32);
    let asnorg = Field::new("asnorg", DataType::String);
    let city = Field::new("city", DataType::String);
    let continent = Field::new("continent", DataType::String);
    let subdivision_iso = Field::new("subdivision_iso", DataType::String);
    let subdivision = Field::new("subdivision", DataType::String);
    let country_iso = Field::new("country_iso", DataType::String);
    let country = Field::new("country", DataType::String);
    let latitude = Field::new("latitude", DataType::Float64);
    let longitude = Field::new("longitude", DataType::Float64);
    let timezone = Field::new("timezone", DataType::String);

    let v: Vec<Field> = vec![
        asnnum,
        asnorg,
        city,
        continent,
        subdivision_iso,
        subdivision,
        country_iso,
        country,
        latitude,
        longitude,
        timezone,
    ];
    Ok(Field::new("", DataType::Struct(v)))
}

// Build struct containing ASN and City level metadata of input IP addresses
#[polars_expr(output_type_func=geoip_full_output)]
fn pl_full_geoip(inputs: &[Series], kwargs: GeoIPKwargs) -> PolarsResult<Series> {
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

    let asn_reader = mdb.asn_reader();
    let city_reader = mdb.city_reader();

    let ca: &StringChunked = inputs[0].str()?;

    let mut asnnum_builder: PrimitiveChunkedBuilder<UInt32Type> =
        PrimitiveChunkedBuilder::new("asnnum", ca.len());
    let mut asnorg_builder = StringChunkedBuilder::new("asnorg", ca.len());
    let mut city_builder = StringChunkedBuilder::new("city", ca.len());
    let mut continent_builder = StringChunkedBuilder::new("continent", ca.len());
    let mut subdivision_iso_builder = StringChunkedBuilder::new("subdivision_iso", ca.len());
    let mut subdivision_builder = StringChunkedBuilder::new("subdivision", ca.len());
    let mut country_iso_builder = StringChunkedBuilder::new("country_iso", ca.len());
    let mut country_builder = StringChunkedBuilder::new("country", ca.len());
    let mut latitude_builder: PrimitiveChunkedBuilder<Float64Type> =
        PrimitiveChunkedBuilder::new("latitude", ca.len());
    let mut longitude_builder: PrimitiveChunkedBuilder<Float64Type> =
        PrimitiveChunkedBuilder::new("longitude", ca.len());
    let mut timezone_builder = StringChunkedBuilder::new("timezone", ca.len());

    ca.into_iter().for_each(|op_s| {
        if let Some(ip_s) = op_s {
            if let Ok(ip) = ip_s.parse::<IpAddr>() {
                let mut asnnum: u32 = 0;
                let mut asnorg: &str = "";
                let mut city: &str = "";
                let mut continent: &str = "";
                let mut subdivision_iso: &str = "";
                let mut subdivision: &str = "";
                let mut country_iso: &str = "";
                let mut country: &str = "";
                let mut latitude: f64 = 0.0;
                let mut longitude: f64 = 0.0;
                let mut timezone: &str = "";

                if let Ok(asnrecord) = asn_reader.lookup::<geoip2::Asn>(ip) {
                    asnnum = asnrecord.autonomous_system_number.unwrap_or(0);
                    asnorg = asnrecord.autonomous_system_organization.unwrap_or("");
                };

                if let Ok(cityrecord) = city_reader.lookup::<geoip2::City>(ip) {
                    // from https://github.com/oschwald/maxminddb-rust/blob/main/examples/within.rs
                    continent = cityrecord.continent.and_then(|c| c.code).unwrap_or("");
                    if let Some(c) = cityrecord.country {
                        country_iso = c.iso_code.unwrap_or("");
                        if let Some(n) = &c.names {
                            country = n.get("en").unwrap_or(&"");
                        }
                    }

                    // Get the first subdivision (if any)
                    if let Some(subdivisions) = cityrecord.subdivisions {
                        if let Some(subdiv) = subdivisions.first() {
                            // Extract subdivision information
                            subdivision_iso = subdiv.iso_code.unwrap_or("");

                            if let Some(subdiv_names) = &subdiv.names {
                                subdivision = subdiv_names.get("en").unwrap_or(&"");
                            }
                        }
                    }

                    // get city name, hard coded for en language currently
                    city = match cityrecord.city.and_then(|c| c.names) {
                        Some(names) => names.get("en").unwrap_or(&""),
                        None => "",
                    };

                    // pull out location specific fields
                    if let Some(locrecord) = cityrecord.location {
                        timezone = locrecord.time_zone.unwrap_or("");
                        latitude = locrecord.latitude.unwrap_or(0.0);
                        longitude = locrecord.longitude.unwrap_or(0.0);
                    };
                };

                // add values to the builders
                asnnum_builder.append_value(asnnum);
                asnorg_builder.append_value(asnorg);
                city_builder.append_value(city);
                continent_builder.append_value(continent);
                subdivision_iso_builder.append_value(subdivision_iso);
                subdivision_builder.append_value(subdivision);
                country_iso_builder.append_value(country_iso);
                country_builder.append_value(country);
                latitude_builder.append_value(latitude);
                longitude_builder.append_value(longitude);
                timezone_builder.append_value(timezone);
            } else {
                // invalid ip, so append nulls for everything
                asnnum_builder.append_null();
                asnorg_builder.append_null();
                city_builder.append_null();
                continent_builder.append_null();
                subdivision_iso_builder.append_null();
                subdivision_builder.append_null();
                country_iso_builder.append_null();
                country_builder.append_null();
                latitude_builder.append_null();
                longitude_builder.append_null();
                timezone_builder.append_null();
            }
        } else {
            // null input, so append nulls for everything
            asnnum_builder.append_null();
            asnorg_builder.append_null();
            city_builder.append_null();
            continent_builder.append_null();
            subdivision_iso_builder.append_null();
            subdivision_builder.append_null();
            country_iso_builder.append_null();
            country_builder.append_null();
            latitude_builder.append_null();
            longitude_builder.append_null();
            timezone_builder.append_null();
        }
    });

    let asnnum_series = asnnum_builder.finish().into_series();
    let asnorg_series = asnorg_builder.finish().into_series();
    let city_series = city_builder.finish().into_series();
    let continent_series = continent_builder.finish().into_series();
    let subdivision_iso_series = subdivision_iso_builder.finish().into_series();
    let subdivision_series = subdivision_builder.finish().into_series();
    let country_iso_series = country_iso_builder.finish().into_series();
    let country_series = country_builder.finish().into_series();
    let latitude_series = latitude_builder.finish().into_series();
    let longitude_series = longitude_builder.finish().into_series();
    let timezone_series = timezone_builder.finish().into_series();

    let out = StructChunked::new(
        "geoip",
        &[
            asnnum_series,
            asnorg_series,
            city_series,
            continent_series,
            subdivision_iso_series,
            subdivision_series,
            country_iso_series,
            country_series,
            latitude_series,
            longitude_series,
            timezone_series,
        ],
    )?;
    Ok(out.into_series())
}

// Get ASN and org name for Internet routed IP addresses
#[polars_expr(output_type=String)]
fn pl_get_asn(inputs: &[Series], kwargs: GeoIPKwargs) -> PolarsResult<Series> {
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

    let out: StringChunked = ca.apply_to_buffer(|value: &str, output: &mut String| {
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
