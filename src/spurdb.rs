#![allow(clippy::unused_unit)]
use lazy_static::lazy_static;
use maxminddb::{Mmap, Reader};
use polars::prelude::*;
use serde::Deserialize;
// use std::borrow::Cow;
use std::env;
use std::io;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

// Define the fields and types that we will support. Since we have a List of variable
// length, we cannot use const, but wrap in lazy_static so we can still import it
// into other modules
lazy_static! {
    pub static ref SPUR_FIELDS: [(&'static str, DataType); 6] = [
        ("client_count", DataType::Float32),
        ("infrastructure", DataType::String),
        ("location_city", DataType::String),
        ("location_country", DataType::String),
        ("location_state", DataType::String),
        // ("services", DataType::List(Box::new(DataType::String))),
        ("tag", DataType::String),
    ];
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

impl<'a> Default for SpurResult<'a> {
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
/// https://docs.spur.us/feeds?id=feed-export-utility
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

// Mutex implementation and error handling improvements provided
// by ChatGPT on 20240717 using GPT-4o
// This instantiates a lazily loaded global connection to Spur
// mmdb database files for re-use
lazy_static! {
    pub static ref SPUR_DB: Mutex<Option<Result<SpurDB, PolarsError>>> = Mutex::new(None);
}

/// Object to hold connections to Spur maxmind MMDB readers
#[derive(Debug)]
pub struct SpurDB {
    spur_reader: Reader<Mmap>,
}

/// Helper function to locate the Spur MMDB directory on the system
/// deferring foremost to the environment variable SPUR_MMDB_DIR and
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
    /// found or mmdb files could not be opened, raise a PolarsCompute
    /// error so it propagates back up to the python user
    fn initialize() -> PolarsResult<Self> {
        let mmdb_dir_result = get_mmdb_dir();

        if mmdb_dir_result.is_err() {
            let error_message = "Error could not locate a directory for Spur MMDB files\n\
                        Hint: specify a directory with the environment variable SPUR_MMDB_DIR\n";
            return Err(PolarsError::ComputeError(error_message.into()));
        }

        let mmdb_dir = mmdb_dir_result.unwrap();

        let spur_path = Path::new(&mmdb_dir).join("spur.mmdb");
        let spur_reader = Reader::open_mmap(&spur_path);

        if spur_reader.is_err() {
            let error_message = format!(
                "Could not open Spur MMDB file from {}",
                spur_path.to_str().unwrap_or_default()
            );
            return Err(PolarsError::ComputeError(error_message.into()));
        }

        Ok(Self {
            spur_reader: spur_reader.unwrap(),
        })
    }

    /// Force a reinitialization of the MMDB readers by dropping
    /// the existing global reader and invoking initialize() again.
    /// This is helpful, particularly in an interactive session (e.g., Jupyter)
    /// and the user has changed SPUR_MMDB_DIR setting or updated
    /// the Spur mmdb files themselves
    pub fn reload() -> PolarsResult<()> {
        let mut db = SPUR_DB.lock().unwrap();
        *db = Some(Self::initialize());
        Ok(())
    }

    /// Modeling OnceLock's get_or_init, gets the global mmdb reader,
    /// initializing it first if necessary
    pub fn get_or_init(
    ) -> PolarsResult<std::sync::MutexGuard<'static, Option<Result<Self, PolarsError>>>> {
        // Credit to GPT-4o for writing this method on 20240717
        let mut db = SPUR_DB.lock().unwrap();
        if db.is_none() {
            *db = Some(Self::initialize());
        }
        Ok(db)
    }

    pub fn iplookup(&self, ip: IpAddr) -> SpurResult<'_> {
        let mut result = SpurResult::default();

        // Lookup spur information
        if let Ok(record) = self.spur_reader.lookup::<SpurLookupResult>(ip) {
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
