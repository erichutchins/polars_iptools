use polars::prelude::*;
use serde::Deserialize;

/// Kwargs struct for Polars expression params
#[derive(Deserialize)]
pub struct MMDBKwargs {
    // geoip expressions should first reload/reinitialize mmdb files
    // before querying
    pub reload_mmdb: bool,
}

/// A trait for initializing Polars builders with a static name and initial capacity.
pub trait BuilderFactory {
    /// Creates a new builder instance.
    fn create(name: &'static str, capacity: usize) -> Self;
}

impl<T: PolarsNumericType> BuilderFactory for PrimitiveChunkedBuilder<T> {
    fn create(name: &'static str, capacity: usize) -> Self {
        PrimitiveChunkedBuilder::<T>::new(PlSmallStr::from_static(name), capacity)
    }
}

impl BuilderFactory for StringChunkedBuilder {
    fn create(name: &'static str, capacity: usize) -> Self {
        StringChunkedBuilder::new(PlSmallStr::from_static(name), capacity)
    }
}

impl BuilderFactory for ListStringChunkedBuilder {
    fn create(name: &'static str, capacity: usize) -> Self {
        // values_capacity initialized to average of 4 strings per list
        ListStringChunkedBuilder::new(PlSmallStr::from_static(name), capacity, capacity * 4)
    }
}

/// A macro to generate a struct-of-builders pattern for Polars DataFrames.
///
/// This macro generates a struct containing multiple Polars `ChunkedBuilder`s,
/// allowing for efficient, type-safe, and DRY row-by-row building of Struct columns.
///
/// # Arguments
///
/// * `name` - The identifier for the generated builder struct (e.g., `MaxmindBuilders`).
/// * `result` - The type of the source data struct being read (e.g., `MaxmindIPResult`).
/// * `arg_name` - The variable name to use for the source struct in append logic closures.
/// * `fields` - A list of field definitions in the format:
///   `field_name: BuilderType => DataType $(, |builder| custom_append_logic)?`
///
/// # Field Definition Styles
///
/// 1. **Simple**: `asnnum: PrimitiveChunkedBuilder<UInt32Type> => DataType::UInt32`
///    Automatically stringifies "asnnum" for the column name and assumes `r.asnnum`
///    is the source value to append.
///
/// 2. **Custom**: `services: ListStringChunkedBuilder => DataType::List(...), |b| { ... }`
///    Allows manually defining how the data is appended to the builder `b`.
///
/// # Generated Methods
///
/// * `new(capacity: usize)` - Initializes all builders with the given capacity.
/// * `append(&mut self, source: &ResultType)` - Appends a row of data from the source struct.
/// * `append_null(&mut self)` - Appends a null value to every builder in the struct.
/// * `finish(self) -> Vec<Series>` - Finalizes all builders into a collection of Series.
/// * `fields() -> Vec<Field>` - Returns the Polars schema (Fields) for the generated struct.
#[macro_export]
macro_rules! make_builders {
    (
        name: $builder_name:ident,
        result: $result_ty:ty,
        arg_name: $arg:ident,
        fields: [
            $(
                $field:ident: $bty:ty => $dtype:expr $(, |$b:ident| $append:expr)?
            ),* $(,)?
        ]
    ) => {
        pub struct $builder_name {
            $(pub $field: $bty,)*
        }

        impl $builder_name {
            /// Initializes a new set of builders with the specified capacity.
            pub fn new(capacity: usize) -> Self {
                Self {
                    $($field: <$bty as $crate::utils::BuilderFactory>::create(stringify!($field), capacity),)*
                }
            }

            /// Appends a single row of data from the source result struct.
            pub fn append(&mut self, $arg: &$result_ty) {
                $(
                    $crate::make_builders!(@dispatch_append self, $arg, $field $(, $b, $append)?);
                )*
            }

            /// Appends a null value to every builder in the struct.
            pub fn append_null(&mut self) {
                $(self.$field.append_null();)*
            }

            /// Finalizes all builders and returns a vector of Series.
            #[allow(unused_mut)]
            pub fn finish(mut self) -> Vec<polars::prelude::Series> {
                vec![
                    $(self.$field.finish().into_series(),)*
                ]
            }

            /// Returns the list of Polars Fields (schema) generated for this builder.
            pub fn fields() -> Vec<polars::prelude::Field> {
                vec![
                    $(
                        polars::prelude::Field::new(
                            polars::prelude::PlSmallStr::from_static(stringify!($field)),
                            $dtype
                        ),
                    )*
                ]
            }
        }
    };

    // Internal helper for custom append logic
    (@dispatch_append $self:ident, $arg:ident, $field:ident, $b:ident, $append:expr) => {
        {
            let $b = &mut $self.$field;
            $append;
        }
    };
    // Internal helper for default append logic
    (@dispatch_append $self:ident, $arg:ident, $field:ident) => {
        $self.$field.append_value($arg.$field);
    };
}
