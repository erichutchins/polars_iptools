#![allow(clippy::unused_unit)]
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use std::net::{IpAddr, Ipv4Addr};
use std::str::FromStr;

/// Returns true if this is a valid ip address
#[polars_expr(output_type=Boolean)]
fn pl_is_ip(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca = s.str()?;

    let out: BooleanChunked =
        ca.apply_nonnull_values_generic(DataType::Boolean, |x| Ipv4Addr::from_str(x).is_ok());
    Ok(out.into_series())
}

/// Returns true if this is a private address defined in IETF RFC 1918
#[polars_expr(output_type=Boolean)]
fn pl_is_private(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca = s.str()?;

    let out: BooleanChunked =
        ca.apply_nonnull_values_generic(DataType::Boolean, |x| match Ipv4Addr::from_str(x) {
            Ok(ip) => ip.is_private(),
            Err(_) => false,
        });
    Ok(out.into_series())
}

/// Convert IPv4 address string to numeric representation
#[polars_expr(output_type=UInt32)]
fn pl_ipv4_to_numeric(inputs: &[Series]) -> PolarsResult<Series> {
    let ca: &StringChunked = inputs[0].str()?;
    let mut builder: PrimitiveChunkedBuilder<UInt32Type> =
        PrimitiveChunkedBuilder::new("ipv4_numeric", ca.len());

    for opt_value in ca.into_iter() {
        if let Some(value) = opt_value {
            match value.parse::<std::net::Ipv4Addr>() {
                Ok(ipv4) => {
                    let num = u32::from(ipv4);
                    builder.append_value(num);
                }
                Err(_) => builder.append_null(), // Handle invalid IPv4 strings
            }
        } else {
            builder.append_null(); // Handle null input values
        }
    }

    Ok(builder.finish().into_series())
}

/// Convert numeric representation of IPv4 address to string
#[polars_expr(output_type=String)]
fn pl_numeric_to_ipv4(inputs: &[Series]) -> PolarsResult<Series> {
    let ca: &UInt32Chunked = inputs[0].u32()?;
    let mut builder = StringChunkedBuilder::new("ipv4_string", ca.len());

    for opt_value in ca.into_iter() {
        if let Some(num) = opt_value {
            let ip = Ipv4Addr::from(num);
            builder.append_value(ip.to_string());
        } else {
            builder.append_null(); // Handle null input values
        }
    }

    Ok(builder.finish().into_series())
}
