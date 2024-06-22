#![allow(clippy::unused_unit)]
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use iptrie::{IpPrefix, RTrieSet};
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use std::net::{IpAddr, Ipv4Addr};
use std::str::FromStr;

/// Returns true if this is a valid IPv4 or IPv6 address
#[polars_expr(output_type=Boolean)]
fn pl_is_valid(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let ca = s.str()?;

    let out: BooleanChunked =
        ca.apply_nonnull_values_generic(DataType::Boolean, |x| IpAddr::from_str(x).is_ok());
    Ok(out.into_series())
}

/// Returns true if this is a private IPv4 address defined in IETF RFC 1918
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

/// Check if IP addresses present in a series of CIDR ranges/prefixes
#[polars_expr(output_type=Boolean)]
fn pl_is_in(inputs: &[Series]) -> PolarsResult<Series> {
    let ca1 = inputs[0].str()?; // ip addresses to lookup
    let ca2 = inputs[1].str()?; // ip networks/cidrs

    let mut ipv4_rtrie: RTrieSet<Ipv4Net> = RTrieSet::with_capacity(ca2.len());
    let mut ipv6_rtrie: RTrieSet<Ipv6Net> = RTrieSet::new();

    // Iterate over ca2, parse as IP range, and add it to the appropriate trie
    for cidr in ca2.into_iter().flatten() {
        match IpNet::from_str(cidr) {
            Ok(IpNet::V4(ipv4)) => {
                _ = ipv4_rtrie.insert(ipv4);
            }
            Ok(IpNet::V6(ipv6)) => {
                _ = ipv6_rtrie.insert(ipv6);
            }
            Err(_) => {
                return Err(PolarsError::ComputeError(
                    format!("Invalid CIDR range: {}", cidr).into(),
                ));
            }
        }
    }

    // Compress the radix trie for faster lookups
    let ipv4_lctrie = ipv4_rtrie.compress();
    let ipv6_lctrie = ipv6_rtrie.compress();

    // Prepare builder to collect results
    let mut builder = BooleanChunkedBuilder::new("is_in", ca1.len());

    for opt_value in ca1.into_iter() {
        if let Some(value) = opt_value {
            match IpAddr::from_str(value) {
                Ok(ip) => {
                    let is_in = match ip {
                        // if the lookups return a nonzero length, we have a matching lookup
                        // otherwise, we don't have a match
                        IpAddr::V4(ipv4) => ipv4_lctrie.lookup(&ipv4).len() > 0,
                        IpAddr::V6(ipv6) => ipv6_lctrie.lookup(&ipv6).len() > 0,
                    };
                    builder.append_value(is_in);
                }
                Err(_) => builder.append_null(), // Handle invalid IP strings
            }
        } else {
            builder.append_null(); // Handle null input values
        }
    }

    Ok(builder.finish().into_series())
}
