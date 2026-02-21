#![allow(clippy::unused_unit)]
use maxminddb::{Mmap, Reader};
use polars::prelude::*;
use serde::Deserialize;
use std::env;
use std::io;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

make_builders! {
    name: SpurBuilders,
    result: SpurResult<'_>,
    arg_name: r,
    fields: [
        client_count: PrimitiveChunkedBuilder<Float32Type> => DataType::Float32,
        infrastructure: StringChunkedBuilder => DataType::String,
        location_city: StringChunkedBuilder => DataType::String,
        location_country: StringChunkedBuilder => DataType::String,
        location_state: StringChunkedBuilder => DataType::String,
        services: ListStringChunkedBuilder => DataType::List(Box::new(DataType::String)), |b| {
            if let Some(s) = &r.services {
                b.append_values_iter(s.iter().copied());
            } else {
                b.append_null();
            }
        },
        tag: StringChunkedBuilder => DataType::String,
    ]
}

#[derive(Debug, Deserialize)]
pub struct SpurResult<'a> {
    pub client_count: f32,
    pub infrastructure: &'a str,
    pub location_city: &'a str,
    pub location_country: &'a str,
    pub location_state: &'a str,
    // declare services as an Option to avoid unnecessary allocations
    pub services: Option<Vec<&'a str>>,
    pub tag: &'a str,
}

impl Default for SpurResult<'_> {
    fn default() -> Self {
        Self {
            client_count: 0.0,
            infrastructure: "",
            location_city: "",
            location_country: "",
            location_state: "",
            services: None,
            tag: "",
        }
    }
}

/// Receive result from maxminddb Reader lookup of Spur database
/// Field name has to match Spur's naming convention
/// <https://docs.spur.us/feeds?id=feed-export-utility>
#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
pub struct SpurLookupResult<'a> {
    pub clientCount: Option<f32>,
    pub infrastructure: Option<&'a str>,
    pub locationCity: Option<&'a str>,
    pub locationCountry: Option<&'a str>,
    pub locationState: Option<&'a str>,
    pub services: Option<Vec<&'a str>>,
    pub tag: Option<&'a str>,
}

// Modern Rust 2021+ approach using OnceLock instead of lazy_static
// This instantiates a lazily loaded global connection to Spur
// mmdb database files for re-use
static SPUR_DB: OnceLock<Mutex<Result<SpurDB, PolarsError>>> = OnceLock::new();

/// Object to hold connections to Spur maxmind MMDB readers
#[derive(Debug)]
pub struct SpurDB {
    spur_reader: Reader<Mmap>,
}

/// Helper function to locate the Spur MMDB directory on the system
/// deferring foremost to the environment variable `SPUR_MMDB_DIR` and
/// then checking two other popular locations (for Mac/Linux systems).
/// Windows clients will have to use the env variable
fn get_mmdb_dir() -> Result<PathBuf, io::Error> {
    // First priority is environment variable
    if let Ok(env_path) = env::var("SPUR_MMDB_DIR") {
        return Ok(PathBuf::from(env_path));
    }

    // List of default paths (on Mac/Linux, at least)
    let default_paths = [
        Path::new("/usr/local/share/GeoIP"),
        Path::new("/opt/homebrew/var/GeoIP"),
    ];

    // Check each default path in order
    for path in &default_paths {
        if path.exists() {
            return Ok(path.to_path_buf());
        }
    }

    // If none of the paths are available, return an error
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "MMDB directory not found in environment variable or default paths",
    ))
}

impl SpurDB {
    /// Initialize the lookup readers by locating directory containing
    /// Spur mmdb files. If directories are not
    /// found or mmdb files could not be opened, raise a `PolarsCompute`
    /// error so it propagates back up to the python user
    fn initialize() -> PolarsResult<Self> {
        let mmdb_dir = get_mmdb_dir().map_err(|_| {
            PolarsError::ComputeError(
                "Error could not locate a directory for Spur MMDB files\n\
                 Hint: specify a directory with the environment variable SPUR_MMDB_DIR\n"
                    .into(),
            )
        })?;

        let spur_path = Path::new(&mmdb_dir).join("spur.mmdb");
        // SAFETY: The mmap'd file is owned by this process and will not be modified
        // or deleted while the reader is alive (static lifetime via OnceLock).
        let spur_reader = unsafe { Reader::open_mmap(&spur_path) }.map_err(|e| {
            PolarsError::ComputeError(
                format!(
                    "Could not open Spur MMDB file from {}: {}",
                    spur_path.to_str().unwrap_or_default(),
                    e
                )
                .into(),
            )
        })?;

        Ok(Self { spur_reader })
    }

    /// Force a reinitialization of the MMDB readers by dropping
    /// the existing global reader and invoking `initialize()` again.
    /// This is helpful, particularly in an interactive session (e.g., Jupyter)
    /// and the user has changed `SPUR_MMDB_DIR` setting or updated
    /// the Spur mmdb files themselves
    pub fn reload() -> PolarsResult<()> {
        let db = SPUR_DB.get_or_init(|| Mutex::new(Self::initialize()));
        let mut guard = db
            .lock()
            .map_err(|_| PolarsError::ComputeError("Failed to acquire SpurDB lock".into()))?;
        *guard = Self::initialize();
        Ok(())
    }

    /// Gets the global mmdb reader, initializing it first if necessary
    pub fn get_or_init() -> PolarsResult<std::sync::MutexGuard<'static, Result<Self, PolarsError>>>
    {
        let db = SPUR_DB.get_or_init(|| Mutex::new(Self::initialize()));
        db.lock()
            .map_err(|_| PolarsError::ComputeError("Failed to acquire SpurDB lock".into()))
    }

    pub fn iplookup(&self, ip: IpAddr) -> SpurResult<'_> {
        let mut result = SpurResult::default();

        // Lookup spur information
        if let Some(record) = self
            .spur_reader
            .lookup(ip)
            .ok()
            .and_then(|lookup| lookup.decode::<SpurLookupResult>().ok().flatten())
        {
            // Populate the SpurLookupResult fields
            result.client_count = record.clientCount.unwrap_or_default();
            result.infrastructure = record.infrastructure.unwrap_or_default();
            result.location_city = record.locationCity.unwrap_or_default();
            result.location_country = record.locationCountry.unwrap_or_default();
            result.location_state = record.locationState.unwrap_or_default();
            result.services = Some(record.services.unwrap_or_default());
            result.tag = record.tag.unwrap_or_default();
        }
        result
    }
}
