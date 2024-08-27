use polars::prelude::*;

pub enum BuilderWrapper {
    UInt32(PrimitiveChunkedBuilder<UInt32Type>),
    Float64(PrimitiveChunkedBuilder<Float64Type>),
    String(StringChunkedBuilder),
}

impl BuilderWrapper {
    pub fn append_value<'a, T>(&mut self, value: T)
    where
        T: Into<AnyValue<'a>>,
    {
        let any_value: AnyValue = value.into();
        match self {
            BuilderWrapper::UInt32(b) => {
                if let AnyValue::UInt32(v) = any_value {
                    b.append_value(v)
                } else {
                    b.append_null()
                }
            }
            BuilderWrapper::Float64(b) => {
                if let AnyValue::Float64(v) = any_value {
                    b.append_value(v)
                } else {
                    b.append_null()
                }
            }
            BuilderWrapper::String(b) => {
                if let AnyValue::String(v) = any_value {
                    b.append_value(v)
                } else {
                    b.append_null()
                }
            }
        }
    }

    pub fn append_null(&mut self) {
        match self {
            BuilderWrapper::UInt32(b) => b.append_null(),
            BuilderWrapper::Float64(b) => b.append_null(),
            BuilderWrapper::String(b) => b.append_null(),
        }
    }

    pub fn finish(self) -> Series {
        match self {
            BuilderWrapper::UInt32(b) => b.finish().into_series(),
            BuilderWrapper::Float64(b) => b.finish().into_series(),
            BuilderWrapper::String(b) => b.finish().into_series(),
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
            DataType::UInt32 => {
                BuilderWrapper::UInt32(PrimitiveChunkedBuilder::<UInt32Type>::new(name, capacity))
            }
            DataType::Float64 => {
                BuilderWrapper::Float64(PrimitiveChunkedBuilder::<Float64Type>::new(name, capacity))
            }
            DataType::String => BuilderWrapper::String(StringChunkedBuilder::new(name, capacity)),
            _ => panic!("Unsupported data type for field: {}", name),
        })
        .collect()
}
