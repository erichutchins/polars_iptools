use ip_extract::{Extractor, ExtractorBuilder, IpMatch};
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use iptrie::{set::RTrieSet, IpPrefix};
use polars::prelude::*;
use polars_core::datatypes::extension::get_extension_type_or_generic;
use pyo3_polars::derive::polars_expr;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr};
use std::str::FromStr;
use std::sync::{Mutex, OnceLock};

static EXTRACTOR_CACHE: OnceLock<Mutex<HashMap<u8, Extractor>>> = OnceLock::new();

/// Build or retrieve a cached Extractor for the given flag combination.
/// Bitmask layout:
///   bit 0 = ipv6
///   bit 1 = only_public
///   bit 2 = ignore_private
///   bit 3 = ignore_loopback
///   bit 4 = ignore_broadcast
fn get_extractor(
    ipv6: bool,
    only_public: bool,
    ignore_private: bool,
    ignore_loopback: bool,
    ignore_broadcast: bool,
) -> &'static Extractor {
    let key = (ipv6 as u8)
        | ((only_public as u8) << 1)
        | ((ignore_private as u8) << 2)
        | ((ignore_loopback as u8) << 3)
        | ((ignore_broadcast as u8) << 4);

    let cache = EXTRACTOR_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = cache.lock().unwrap();

    map.entry(key).or_insert_with(|| {
        let mut builder = ExtractorBuilder::new();
        builder.ipv6(ipv6);
        if only_public {
            builder.only_public();
        }
        if ignore_private {
            builder.ignore_private();
        }
        if ignore_loopback {
            builder.ignore_loopback();
        }
        if ignore_broadcast {
            builder.ignore_broadcast();
        }
        builder.build().expect("BUG: failed to build extractor")
    });

    // SAFETY: HashMap entries are never removed, so the reference is valid for 'static.
    let ptr: *const Extractor = map.get(&key).unwrap();
    unsafe { &*ptr }
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

/// Extract IP addresses from a string column with optional filtering.
#[polars_expr(output_type_func=extract_ips_output)]
fn pl_extract_ips(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let ipv6 = inputs[1].bool()?.get(0).unwrap_or(false);
    let only_public = inputs[2].bool()?.get(0).unwrap_or(false);
    let ignore_private = inputs[3].bool()?.get(0).unwrap_or(false);
    let ignore_loopback = inputs[4].bool()?.get(0).unwrap_or(false);
    let ignore_broadcast = inputs[5].bool()?.get(0).unwrap_or(false);

    let extractor = get_extractor(
        ipv6,
        only_public,
        ignore_private,
        ignore_loopback,
        ignore_broadcast,
    );

    let mut builder =
        ListStringChunkedBuilder::new(PlSmallStr::from_static("ips"), ca.len(), ca.len() * 2);

    for opt_val in ca {
        match opt_val {
            Some(val) => {
                let matches: Vec<String> = extractor
                    .match_iter(val.as_bytes())
                    .map(|m: IpMatch| m.as_str().into_owned())
                    .collect();
                builder.append_values_iter(matches.iter().map(String::as_str));
            },
            None => builder.append_null(),
        }
    }

    Ok(builder.finish().into_series())
}

/// Extract only private IP addresses from a string column.
#[polars_expr(output_type_func=extract_ips_output)]
fn pl_extract_private_ips(inputs: &[Series]) -> PolarsResult<Series> {
    let ca = inputs[0].str()?;
    let ipv6 = inputs[1].bool()?.get(0).unwrap_or(false);

    // Use an extractor without private filter, then post-filter for private only
    let extractor = get_extractor(ipv6, false, false, true, true);

    let mut builder =
        ListStringChunkedBuilder::new(PlSmallStr::from_static("ips"), ca.len(), ca.len() * 2);

    for opt_val in ca {
        match opt_val {
            Some(val) => {
                let matches: Vec<String> = extractor
                    .match_iter(val.as_bytes())
                    .filter(|m| {
                        let ip = m.ip();
                        match ip {
                            IpAddr::V4(v4) => v4.is_private(),
                            IpAddr::V6(v6) => {
                                // ULA: fc00::/7
                                let seg = v6.segments();
                                (seg[0] & 0xfe00) == 0xfc00
                            },
                        }
                    })
                    .map(|m| m.as_str().into_owned())
                    .collect();
                builder.append_values_iter(matches.iter().map(String::as_str));
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

/// Per-row outcome when converting a Series to IP addresses.
pub enum IpEntry {
    /// Source value was null.
    Null,
    /// Source value was non-null but could not be parsed as an IP address.
    Invalid,
    /// Successfully resolved IP address.
    Addr(IpAddr),
}

/// Convert any IP-typed Series (String, IPv4/UInt32, or IPAddress/Binary) to a
/// `Vec<IpEntry>` so that geoip/spur functions can accept all three input types
/// without converting back through strings.
///
/// The `IpEntry` enum preserves the distinction between a null source value
/// (`IpEntry::Null`) and a non-null unparseable value (`IpEntry::Invalid`),
/// allowing callers to emit different outputs for each case.
///
/// Extension wrapper types (DataType::Extension) are transparently stripped via
/// `to_physical_repr()` before dispatch.
pub fn series_to_ipaddrs(s: &Series) -> PolarsResult<Vec<IpEntry>> {
    // Strip extension wrapper if present (IPv4 or IPAddress extension types)
    let physical = s.to_physical_repr();
    match physical.dtype() {
        DataType::String => {
            let ca = physical.str()?;
            Ok(ca
                .into_iter()
                .map(|opt| match opt {
                    None => IpEntry::Null,
                    Some(v) => match v.parse::<IpAddr>() {
                        Ok(ip) => IpEntry::Addr(ip),
                        Err(_) => IpEntry::Invalid,
                    },
                })
                .collect())
        },
        DataType::UInt32 => {
            // IPv4 extension storage: u32 representing IPv4 address
            let ca = physical.u32()?;
            Ok(ca
                .into_iter()
                .map(|opt| match opt {
                    None => IpEntry::Null,
                    Some(n) => IpEntry::Addr(IpAddr::V4(Ipv4Addr::from(n))),
                })
                .collect())
        },
        DataType::Binary => {
            // IPAddress extension storage: 16-byte network-order IPv6 (with
            // IPv4-mapped addresses stored as ::ffff:a.b.c.d)
            let ca = physical.binary()?;
            Ok(ca
                .into_iter()
                .map(|opt| match opt {
                    None => IpEntry::Null,
                    Some(bytes) => {
                        if bytes.len() == 16 {
                            let mut octets = [0u8; 16];
                            octets.copy_from_slice(bytes);
                            let ip6 = std::net::Ipv6Addr::from(octets);
                            let ip = match ip6.to_ipv4_mapped() {
                                Some(ip4) => IpAddr::V4(ip4),
                                None => IpAddr::V6(ip6),
                            };
                            IpEntry::Addr(ip)
                        } else {
                            IpEntry::Invalid
                        }
                    },
                })
                .collect())
        },
        dt => polars_bail!(
            InvalidOperation: "IP lookup expects String, IPv4, or IPAddress column; got {}",
            dt
        ),
    }
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
