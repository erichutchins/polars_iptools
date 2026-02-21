use polars::{chunked_array::builder::NullChunkedBuilder, prelude::*};
use serde::Deserialize;

/// Kwargs struct for Polars expression params
#[derive(Deserialize)]
pub struct MMDBKwargs {
    // geoip expressions should first reload/reinitialize mmdb files
    // before querying
    pub reload_mmdb: bool,
}

/// `BuilderWrapper` is an enum that wraps different types of Polars `ChunkedBuilders`.
/// It provides a unified interface for appending values and handling nulls across
/// different data types, simplifying the process of building Series with mixed types.
/// This allows for creating a vec/array of disparate builder types, enabling
/// flexible handling of multiple data types within a single collection.
pub enum BuilderWrapper {
    UInt32(PrimitiveChunkedBuilder<UInt32Type>),
    Float32(PrimitiveChunkedBuilder<Float32Type>),
    Float64(PrimitiveChunkedBuilder<Float64Type>),
    String(StringChunkedBuilder),
    ListString(ListStringChunkedBuilder),
    Invalid(NullChunkedBuilder),
}

impl BuilderWrapper {
    #[inline]
    pub fn append_value<'a, T>(&mut self, value: T) -> PolarsResult<()>
    where
        T: Into<AnyValue<'a>>,
    {
        let any_value: AnyValue = value.into();
        match self {
            BuilderWrapper::UInt32(b) => {
                if let AnyValue::UInt32(v) = any_value {
                    b.append_value(v);
                } else {
                    b.append_null();
                }
            },
            BuilderWrapper::Float32(b) => {
                if let AnyValue::Float32(v) = any_value {
                    b.append_value(v);
                } else {
                    b.append_null();
                }
            },
            BuilderWrapper::Float64(b) => {
                if let AnyValue::Float64(v) = any_value {
                    b.append_value(v);
                } else {
                    b.append_null();
                }
            },
            BuilderWrapper::String(b) => {
                if let AnyValue::String(v) = any_value {
                    b.append_value(v);
                } else {
                    b.append_null();
                }
            },
            BuilderWrapper::ListString(_) => {
                polars_bail!(InvalidOperation: "Use append_option_string_vec for ListString variant, not append_value")
            },
            BuilderWrapper::Invalid(b) => b.append_null(),
        }
        Ok(())
    }

    /// Specialized method for appending Option<&Vec<&str>> to `ListString` builders
    #[inline]
    pub fn append_option_string_vec(&mut self, opt_vec: Option<&Vec<&str>>) -> PolarsResult<()> {
        match self {
            BuilderWrapper::ListString(b) => {
                if let Some(vec) = opt_vec {
                    b.append_values_iter(vec.iter().copied());
                } else {
                    b.append_null();
                }
            },
            _ => {
                polars_bail!(InvalidOperation: "append_option_string_vec called on non-ListString variant")
            },
        }
        Ok(())
    }

    #[inline]
    pub fn append_null(&mut self) {
        match self {
            BuilderWrapper::UInt32(b) => b.append_null(),
            BuilderWrapper::Float32(b) => b.append_null(),
            BuilderWrapper::Float64(b) => b.append_null(),
            BuilderWrapper::String(b) => b.append_null(),
            BuilderWrapper::ListString(b) => b.append_null(),
            BuilderWrapper::Invalid(b) => b.append_null(),
        }
    }

    pub fn finish(self) -> Series {
        match self {
            BuilderWrapper::UInt32(b) => b.finish().into_series(),
            BuilderWrapper::Float32(b) => b.finish().into_series(),
            BuilderWrapper::Float64(b) => b.finish().into_series(),
            BuilderWrapper::String(b) => b.finish().into_series(),
            BuilderWrapper::ListString(mut b) => b.finish().into_series(),
            BuilderWrapper::Invalid(b) => b.finish().into_series(),
        }
    }
}

pub fn create_builders<'a, const N: usize>(
    fields: &'a [(&'a str, DataType); N],
    capacity: usize,
) -> Vec<BuilderWrapper> {
    fields
        .iter()
        .map(|(name, dtype)| match dtype {
            DataType::UInt32 => BuilderWrapper::UInt32(PrimitiveChunkedBuilder::<UInt32Type>::new(
                PlSmallStr::from_str(name),
                capacity,
            )),
            DataType::Float32 => BuilderWrapper::Float32(
                PrimitiveChunkedBuilder::<Float32Type>::new(PlSmallStr::from_str(name), capacity),
            ),
            DataType::Float64 => BuilderWrapper::Float64(
                PrimitiveChunkedBuilder::<Float64Type>::new(PlSmallStr::from_str(name), capacity),
            ),
            DataType::String => BuilderWrapper::String(StringChunkedBuilder::new(
                PlSmallStr::from_str(name),
                capacity,
            )),
            DataType::List(inner_type) if matches!(**inner_type, DataType::String) => {
                BuilderWrapper::ListString(ListStringChunkedBuilder::new(
                    PlSmallStr::from_str(name),
                    capacity,
                    4, // initial values_capacity per list
                ))
            },
            _ => {
                let error_name = format!("{name}_error");
                BuilderWrapper::Invalid(NullChunkedBuilder::new(
                    PlSmallStr::from_str(error_name.as_str()),
                    capacity,
                ))
            },
        })
        .collect()
}
