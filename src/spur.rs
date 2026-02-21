#![allow(clippy::unused_unit)]
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use std::net::IpAddr;

use crate::spurdb::{spur_fields, SpurDB};
use crate::utils::{create_builders, BuilderWrapper, MMDBKwargs};

// borrowing pattern from github.com/abstractqqq/polars_istr
fn spur_full_output(_: &[Field]) -> PolarsResult<Field> {
    let spur_fields = spur_fields();
    let v: Vec<Field> = spur_fields
        .iter()
        .map(|(name, data_type)| Field::new(PlSmallStr::from_str(name), data_type.clone()))
        .collect();

    Ok(Field::new(PlSmallStr::EMPTY, DataType::Struct(v)))
}

// Build struct containing Spur IP Context for Anonymous or Anonymous+Residential
// metadata of input IP addresses
#[polars_expr(output_type_func=spur_full_output)]
fn pl_full_spur(inputs: &[Series], kwargs: MMDBKwargs) -> PolarsResult<Series> {
    if kwargs.reload_mmdb {
        SpurDB::reload()?;
    }

    let guard = SpurDB::get_or_init()?;
    let mdb = guard.as_ref().map_err(|e| {
        PolarsError::ComputeError(format!("Failed to initialize SpurDB: {e}").into())
    })?;

    let ca: &StringChunked = inputs[0].str()?;

    // Create builders for all fields, including ListString for 'services'
    let spur_fields = spur_fields();
    let mut builders = create_builders(&spur_fields, ca.len());

    for op_s in ca.into_iter() {
        if let Some(ip_s) = op_s {
            if let Ok(ip) = ip_s.parse::<IpAddr>() {
                let spuripresult = mdb.iplookup(ip);

                // Important: these must be in same order as spur_fields()
                builders[0].append_value(spuripresult.client_count)?;
                builders[1].append_value(spuripresult.infrastructure)?;
                builders[2].append_value(spuripresult.location_city)?;
                builders[3].append_value(spuripresult.location_country)?;
                builders[4].append_value(spuripresult.location_state)?;
                builders[5].append_option_string_vec(spuripresult.services.as_ref())?;
                builders[6].append_value(spuripresult.tag)?;
            } else {
                for builder in &mut builders {
                    builder.append_null();
                }
            }
        } else {
            for builder in &mut builders {
                builder.append_null();
            }
        }
    }

    // Finalize all builders into series
    let series: Vec<Series> = builders.into_iter().map(BuilderWrapper::finish).collect();

    StructChunked::from_series(PlSmallStr::from_static("spur"), ca.len(), series.iter())
        .map(IntoSeries::into_series)
}
