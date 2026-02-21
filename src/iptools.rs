use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use iptrie::{set::RTrieSet, IpPrefix};
use polars::prelude::*;
use polars_core::datatypes::extension::get_extension_type_or_generic;
use pyo3_polars::derive::polars_expr;
use regex::Regex;
use std::net::{IpAddr, Ipv4Addr};
use std::str::FromStr;
use std::sync::OnceLock;

static IPV4_RE: OnceLock<Regex> = OnceLock::new();
static ALL_IP_RE: OnceLock<Regex> = OnceLock::new();

const IPV4_PATT: &str =
    r"((?:(?:\d|[01]?\d\d|2[0-4]\d|25[0-5])\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d|\d))";
const IPV6_PATT: &str = r"((?:(?:(?:(?:[0-9a-fA-F]){1,4}):){1,4}:[^\s:](?:(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9]).){3,3}(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])))|(?:::(?:ffff(?::0{1,4}){0,1}:){0,1}[^\s:](?:(?:(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9]).){3,3}(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])))|(?:fe80:(?::(?:(?:[0-9a-fA-F]){1,4})){0,4}%[0-9a-zA-Z]{1,})|(?::(?:(?::(?:(?:[0-9a-fA-F]){1,4})){1,7}|:))|(?:(?:(?:[0-9a-fA-F]){1,4}):(?:(?::(?:(?:[0-9a-fA-F]){1,4})){1,6}))|(?:(?:(?:(?:[0-9a-fA-F]){1,4}):){1,2}(?::(?:(?:[0-9a-fA-F]){1,4})){1,5})|(?:(?:(?:(?:[0-9a-fA-F]){1,4}):){1,3}(?::(?:(?:[0-9a-fA-F]){1,4})){1,4})|(?:(?:(?:(?:[0-9a-fA-F]){1,4}):){1,4}(?::(?:(?:[0-9a-fA-F]){1,4})){1,3})|(?:(?:(?:(?:[0-9a-fA-F]){1,4}):){1,5}(?::(?:(?:[0-9a-fA-F]){1,4})){1,2})|(?:(?:(?:(?:[0-9a-fA-F]){1,4}):){1,6}:(?:(?:[0-9a-fA-F]){1,4}))|(?:(?:(?:(?:[0-9a-fA-F]){1,4}):){1,7}:)|(?:(?:(?:(?:[0-9a-fA-F]){1,4}):){7,7}(?:(?:[0-9a-fA-F]){1,4})))";

fn get_ipv4_re() -> &'static Regex {
    IPV4_RE.get_or_init(|| {
        Regex::new(IPV4_PATT).expect("BUG: compiled-in IPv4 regex pattern is invalid")
    })
}

fn get_all_ip_re() -> &'static Regex {
    ALL_IP_RE.get_or_init(|| {
        let patt = format!("{}|{}", IPV4_PATT, IPV6_PATT);
        Regex::new(&patt).expect("BUG: compiled-in IP regex pattern is invalid")
    })
}

/// Returns true if this is a valid IPv4 or IPv6 address
#[polars_expr(output_type=Boolean)]
fn pl_is_valid(inputs: &[Series]) -> PolarsResult<Series> {
    let ca: &StringChunked = inputs[0].str()?;
    let out: BooleanChunked =
        ca.apply_nonnull_values_generic(DataType::Boolean, |x| IpAddr::from_str(x).is_ok());
    Ok(out.into_series())
}

/// Returns true if this is a private IPv4 address defined in IETF RFC 1918
#[polars_expr(output_type=Boolean)]
fn pl_is_private(inputs: &[Series]) -> PolarsResult<Series> {
    let ca: &StringChunked = inputs[0].str()?;
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
        PrimitiveChunkedBuilder::new(PlSmallStr::from_static("ipv4_numeric"), ca.len());

    for opt_value in ca {
        if let Some(value) = opt_value {
            match value.parse::<std::net::Ipv4Addr>() {
                Ok(ipv4) => {
                    let num = u32::from(ipv4);
                    builder.append_value(num);
                },
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
    let binding = inputs[0].to_physical_repr();
    let ca = binding.u32()?;
    let mut builder = StringChunkedBuilder::new(PlSmallStr::from_static("ipv4_string"), ca.len());

    for opt_value in ca {
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
    let ca1: &StringChunked = inputs[0].str()?; // ip addresses to lookup
    let ca2: &StringChunked = inputs[1].str()?; // ip networks/cidrs

    let mut ipv4_rtrie: RTrieSet<Ipv4Net> = RTrieSet::with_capacity(ca2.len());
    // We expect less ipv6 ips than ipv4 so only allocate half the input length
    let mut ipv6_rtrie: RTrieSet<Ipv6Net> = RTrieSet::with_capacity(ca2.len() / 2);

    // Iterate over ca2, parse as IP range, and add it to the appropriate trie
    for cidr in ca2.into_iter().flatten() {
        match IpNet::from_str(cidr) {
            Ok(IpNet::V4(ipv4)) => {
                ipv4_rtrie.insert(ipv4);
            },
            Ok(IpNet::V6(ipv6)) => {
                ipv6_rtrie.insert(ipv6);
            },
            Err(_) => {
                polars_bail!(InvalidOperation: "Invalid CIDR range: {}", cidr);
            },
        }
    }

    // Compress the radix trie for faster lookups
    let ipv4_lctrie = ipv4_rtrie.compress();
    let ipv6_lctrie = ipv6_rtrie.compress();

    // Prepare builder to collect results
    let mut builder = BooleanChunkedBuilder::new(PlSmallStr::from_static("is_in"), ca1.len());

    for opt_value in ca1 {
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
                },
                Err(_) => builder.append_null(), // Handle invalid IP strings
            }
        } else {
            builder.append_null(); // Handle null input values
        }
    }

    Ok(builder.finish().into_series())
}

fn extract_ips_output(_: &[Field]) -> PolarsResult<Field> {
    Ok(Field::new(
        PlSmallStr::from_static("ips"),
        DataType::List(Box::new(DataType::String)),
    ))
}

/// Extract all IP addresses from a string column
#[polars_expr(output_type_func=extract_ips_output)]
fn pl_extract_all_ips(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let ipv6_series = &inputs[1];
    let ipv6 = ipv6_series.bool()?.get(0).unwrap_or(false);

    let re = if ipv6 { get_all_ip_re() } else { get_ipv4_re() };

    let mut builder =
        ListStringChunkedBuilder::new(PlSmallStr::from_static("ips"), ca.len(), ca.len() * 2);

    for opt_val in ca {
        match opt_val {
            Some(val) => {
                // Stream matches directly into the builder to avoid temporary Vec allocations
                builder.append_values_iter(re.find_iter(val).map(|m| m.as_str()));
            },
            None => builder.append_null(),
        }
    }

    Ok(builder.finish().into_series())
}

/// Convert IP Address Extension Type (either IPv4 or IPAddress) to string
#[polars_expr(output_type=String)]
fn pl_ip_to_str(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];

    match s.dtype() {
        DataType::UInt32 => {
            let ca = s.u32()?;
            let mut builder =
                StringChunkedBuilder::new(PlSmallStr::from_static("ip_string"), ca.len());
            for opt_value in ca {
                if let Some(num) = opt_value {
                    builder.append_value(Ipv4Addr::from(num).to_string());
                } else {
                    builder.append_null();
                }
            }
            Ok(builder.finish().into_series())
        },
        DataType::Binary => {
            let ca = s.binary()?;
            let mut builder =
                StringChunkedBuilder::new(PlSmallStr::from_static("ip_string"), ca.len());
            for opt_value in ca {
                if let Some(bytes) = opt_value {
                    if bytes.len() == 16 {
                        let mut octets = [0u8; 16];
                        octets.copy_from_slice(bytes);
                        let ip = std::net::Ipv6Addr::from(octets);

                        // Check for IPv4-mapped address: ::ffff:a.b.c.d
                        let segments = ip.segments();
                        if segments[0] == 0
                            && segments[1] == 0
                            && segments[2] == 0
                            && segments[3] == 0
                            && segments[4] == 0
                            && segments[5] == 0xffff
                        {
                            let ipv4 = std::net::Ipv4Addr::new(
                                (segments[6] >> 8) as u8,
                                (segments[6] & 0xff) as u8,
                                (segments[7] >> 8) as u8,
                                (segments[7] & 0xff) as u8,
                            );
                            builder.append_value(ipv4.to_string());
                        } else {
                            builder.append_value(ip.to_string());
                        }
                    } else if bytes.len() == 4 {
                        let mut octets = [0u8; 4];
                        octets.copy_from_slice(bytes);
                        builder.append_value(std::net::Ipv4Addr::from(octets).to_string());
                    } else {
                        builder.append_null();
                    }
                } else {
                    builder.append_null();
                }
            }
            Ok(builder.finish().into_series())
        },
        _ => polars_bail!(InvalidOperation: "to_canonical only supports UInt32 or Binary storage"),
    }
}

/// Promote any compatible type (u32, string, binary) to Unified IPAddress Extension Type
#[polars_expr(output_type_func=ip_address_dtype)]
fn pl_to_ip(inputs: &[Series]) -> PolarsResult<Series> {
    let s = &inputs[0];
    let out = match s.dtype() {
        DataType::UInt32 => {
            let ca = s.u32()?;
            let mut builder =
                BinaryChunkedBuilder::new(PlSmallStr::from_static("ip_address"), ca.len());
            for opt_val in ca {
                if let Some(num) = opt_val {
                    let ipv6 = std::net::Ipv4Addr::from(num).to_ipv6_mapped();
                    builder.append_value(ipv6.octets().as_slice());
                } else {
                    builder.append_null();
                }
            }
            builder.finish().into_series()
        },
        DataType::Int64 | DataType::Int32 => {
            let s_u32 = s.cast(&DataType::UInt32)?;
            let ca = s_u32.u32()?;
            let mut builder =
                BinaryChunkedBuilder::new(PlSmallStr::from_static("ip_address"), ca.len());
            for opt_val in ca {
                if let Some(num) = opt_val {
                    let ipv6 = std::net::Ipv4Addr::from(num).to_ipv6_mapped();
                    builder.append_value(ipv6.octets().as_slice());
                } else {
                    builder.append_null();
                }
            }
            builder.finish().into_series()
        },
        DataType::String => {
            let ca = s.str()?;
            let mut builder =
                BinaryChunkedBuilder::new(PlSmallStr::from_static("ip_address"), ca.len());
            for opt_val in ca {
                if let Some(val) = opt_val {
                    if let Ok(ipv4) = val.parse::<std::net::Ipv4Addr>() {
                        builder.append_value(ipv4.to_ipv6_mapped().octets().as_slice());
                    } else if let Ok(ipv6) = val.parse::<std::net::Ipv6Addr>() {
                        builder.append_value(ipv6.octets().as_slice());
                    } else {
                        builder.append_null();
                    }
                } else {
                    builder.append_null();
                }
            }
            builder.finish().into_series()
        },
        DataType::Binary => s.clone(),
        _ => polars_bail!(InvalidOperation: "to_ip only supports UInt32, String, or Binary inputs"),
    };

    out.cast(&ip_address_ext_dtype())
}

/// Helper to get the IPv4 extension datatype in Rust
fn ipv4_ext_dtype() -> DataType {
    let ext_instance =
        get_extension_type_or_generic("polars_iptools.ipv4", &DataType::UInt32, None);
    DataType::Extension(ext_instance, Box::new(DataType::UInt32))
}

/// Helper to get the unified IPAddress extension datatype in Rust
fn ip_address_ext_dtype() -> DataType {
    let ext_instance =
        get_extension_type_or_generic("polars_iptools.ip_address", &DataType::Binary, None);
    DataType::Extension(ext_instance, Box::new(DataType::Binary))
}

pub fn ipv4_dtype(_: &[Field]) -> PolarsResult<Field> {
    Ok(Field::new(
        PlSmallStr::from_static("ipv4"),
        ipv4_ext_dtype(),
    ))
}

pub fn ip_address_dtype(_: &[Field]) -> PolarsResult<Field> {
    Ok(Field::new(
        PlSmallStr::from_static("ip_address"),
        ip_address_ext_dtype(),
    ))
}

/// Parse string column into IPv4 Extension Type
#[polars_expr(output_type_func=ipv4_dtype)]
fn pl_ipv4_from_str(inputs: &[Series]) -> PolarsResult<Series> {
    let ca: &StringChunked = inputs[0].str()?;
    let mut builder =
        PrimitiveChunkedBuilder::<UInt32Type>::new(PlSmallStr::from_static("ipv4"), ca.len());

    for opt_value in ca {
        if let Some(value) = opt_value {
            match value.parse::<std::net::Ipv4Addr>() {
                Ok(ipv4) => builder.append_value(u32::from(ipv4)),
                Err(_) => builder.append_null(),
            }
        } else {
            builder.append_null();
        }
    }

    let out = builder.finish().into_series();
    out.cast(&ipv4_ext_dtype())
}
