#![allow(clippy::unused_unit)]
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

use crate::iptools::{series_to_ipaddrs, IpEntry};
use crate::spurdb::{SpurBuilders, SpurDB};
use crate::utils::MMDBKwargs;

// borrowing pattern from github.com/abstractqqq/polars_istr
fn spur_full_output(_: &[Field]) -> PolarsResult<Field> {
    Ok(Field::new(
        PlSmallStr::EMPTY,
        DataType::Struct(SpurBuilders::fields()),
    ))
}

// Build struct containing Spur IP Context for Anonymous or Anonymous+Residential
// metadata of input IP addresses.
// Accepts String, IPv4 (UInt32 storage), or IPAddress (Binary storage) columns.
#[polars_expr(output_type_func=spur_full_output)]
fn pl_full_spur(inputs: &[Series], kwargs: MMDBKwargs) -> PolarsResult<Series> {
    if kwargs.reload_mmdb {
        SpurDB::reload()?;
    }

    let guard = SpurDB::get_or_init()?;
    let mdb = guard.as_ref().map_err(|e| {
        PolarsError::ComputeError(format!("Failed to initialize SpurDB: {e}").into())
    })?;

    let ips = series_to_ipaddrs(&inputs[0])?;

    let mut builders = SpurBuilders::new(ips.len());

    for entry in &ips {
        match entry {
            IpEntry::Addr(ip) => {
                let spuripresult = mdb.iplookup(*ip);
                builders.append(&spuripresult);
            },
            IpEntry::Null | IpEntry::Invalid => {
                builders.append_null();
            },
        }
    }

    // Finalize all builders into series
    let series: Vec<Series> = builders.finish();

    StructChunked::from_series(PlSmallStr::from_static("spur"), ips.len(), series.iter())
        .map(IntoSeries::into_series)
}
