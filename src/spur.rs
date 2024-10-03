#![allow(clippy::unused_unit)]
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use std::net::IpAddr;

use crate::spurdb::{SpurDB, SPUR_FIELDS};
use crate::utils::{create_builders, MMDBKwargs};

// borrowing pattern from github.com/abstractqqq/polars_istr
fn spur_full_output(_: &[Field]) -> PolarsResult<Field> {
    let v: Vec<Field> = SPUR_FIELDS
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

    let binding = SpurDB::get_or_init()?;
    let mdb = binding
            .as_ref()
            .ok_or_else(|| PolarsError::ComputeError("Error: SpurDB is not initialized. Please ensure that the MMDB files are correctly placed and accessible.".into()))?
            .as_ref()
            .map_err(|e| {
                PolarsError::ComputeError(format!("Failed to initialize SpurDB: {}", e).into())
            })?;

    let ca: &StringChunked = inputs[0].str()?;

    // Create builders for all fields except 'services'
    let mut builders = create_builders(&SPUR_FIELDS, ca.len());
    // Note: ListStringChunkedBuilder is created separately as adding it to the BuilderWrapper enum
    // was too complicated for my rust skills. Each List is initialized with a capacity of 4, which is a
    // generous estimate for the expected number of services per IP.
    let mut services_builder =
        ListStringChunkedBuilder::new(PlSmallStr::from("services"), ca.len(), 4);

    ca.into_iter().for_each(|op_s| {
        if let Some(ip_s) = op_s {
            if let Ok(ip) = ip_s.parse::<IpAddr>() {
                let spuripresult = mdb.iplookup(ip);

                // add values to the builders
                // Important: these must be in same order as SPUR_FIELDS
                // sort alphabetically to ensure eas(ier) maintenance
                builders[0].append_value(spuripresult.client_count);
                builders[1].append_value(spuripresult.infrastructure);
                builders[2].append_value(spuripresult.location_city);
                builders[3].append_value(spuripresult.location_country);
                builders[4].append_value(spuripresult.location_state);
                //builders[5].append_value(spuripresult.services);
                builders[5].append_value(spuripresult.tag);

                // Add the services from the Option<Vec> into the standalone builder
                if let Some(services) = &spuripresult.services {
                    services_builder.append_values_iter(services.iter().copied());
                } else {
                    services_builder.append_null();
                }
            } else {
                // invalid ip, so append nulls for everything
                builders
                    .iter_mut()
                    .for_each(|builder| builder.append_null());
                services_builder.append_null();
            }
        } else {
            // null input, so append nulls for everything
            builders
                .iter_mut()
                .for_each(|builder| builder.append_null());
            services_builder.append_null();
        }
    });

    // finalize builders and instantiate resulting Struct
    let mut series: Vec<Series> = builders.into_iter().map(|b| b.finish()).collect();
    series.push(services_builder.finish().into_series());
    StructChunked::from_series(PlSmallStr::from("spur"), &series).map(|ca| ca.into_series())
}
