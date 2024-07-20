#![allow(clippy::unused_unit)]
use lazy_static::lazy_static;
use maxminddb::{Mmap, Reader};
use polars::prelude::*;
use serde::Deserialize;
use std::env;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

// Mutex implementation and error handling improvements provided
// by ChatGPT on 20240717 using GPT-4o
// This instantiates a lazily loaded global connection to MaxMind
// mmdb database files for re-use
lazy_static! {
    pub static ref MAXMIND_DB: Mutex<Option<Result<MaxMindDB, PolarsError>>> = Mutex::new(None);
}

/// Object to hold connections to ASN and City MaxMind MMDB readers
#[derive(Debug)]
pub struct MaxMindDB {
    asn_reader: Reader<Mmap>,
    city_reader: Reader<Mmap>,
}

/// Kwargs struct for Polars expression params
#[derive(Deserialize)]
pub struct GeoIPKwargs {
    // geoip expressions should first reload/reinitialize mmdb files
    // before querying
    pub reload_mmdb: bool,
}

/// Helper function to locate the MaxMind MMDB directory on the system
/// deferring foremost to the environment variable MAXMIND_MMDB_DIR and
/// then checking two other popular locations (for Mac/Linux systems).
/// Windows clients will have to use the env variable
fn get_mmdb_dir() -> Result<PathBuf, io::Error> {
    // First priority is environment variable
    if let Ok(env_path) = env::var("MAXMIND_MMDB_DIR") {
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

impl MaxMindDB {
    /// Initialize the lookup readers by locating directory containing
    /// MaxMind mmdb files and opening ASN and City readers. If directories are not
    /// found or mmdb files could not be opened, raise a PolarsCompute
    /// error so it propagates back up to the python user
    fn initialize() -> PolarsResult<Self> {
        let mmdb_dir_result = get_mmdb_dir();

        if mmdb_dir_result.is_err() {
            let error_message = "Error could not locate a directory for MaxMind MMDB files\n\
                        Hint: specify a directory with the environment variable MAXMIND_MMDB_DIR\n";
            return Err(PolarsError::ComputeError(error_message.into()));
        }

        let mmdb_dir = mmdb_dir_result.unwrap();

        let asn_path = Path::new(&mmdb_dir).join("GeoLite2-ASN.mmdb");
        let city_path = Path::new(&mmdb_dir).join("GeoLite2-City.mmdb");

        let asn_reader = Reader::open_mmap(&asn_path);
        let city_reader = Reader::open_mmap(&city_path);

        if asn_reader.is_err() {
            let error_message = format!(
                "Could not open ASN MMDB file from {}",
                asn_path.to_str().unwrap_or_default()
            );
            return Err(PolarsError::ComputeError(error_message.into()));
        }

        if city_reader.is_err() {
            let error_message = format!(
                "Could not open City MMDB file from {}",
                city_path.to_str().unwrap_or_default()
            );
            return Err(PolarsError::ComputeError(error_message.into()));
        }

        Ok(Self {
            asn_reader: asn_reader.unwrap(),
            city_reader: city_reader.unwrap(),
        })
    }

    /// Force a reinitialization of the MMDB readers by dropping
    /// the existing global reader and invoking initialize() again.
    /// This is helpful, particularly in an interactive session (e.g., Jupyter)
    /// and the user has changed MAXMIND_MMDB_DIR setting or updated
    /// the MaxMind mmdb files themselves
    pub fn reload() -> PolarsResult<()> {
        let mut db = MAXMIND_DB.lock().unwrap();
        *db = Some(Self::initialize());
        Ok(())
    }

    /// Modeling OnceLock's get_or_init, gets the global mmdb reader,
    /// initializing it first if necessary
    pub fn get_or_init(
    ) -> PolarsResult<std::sync::MutexGuard<'static, Option<Result<Self, PolarsError>>>> {
        // Credit to GPT-4o for writing this method on 20240717
        let mut db = MAXMIND_DB.lock().unwrap();
        if db.is_none() {
            *db = Some(Self::initialize());
        }
        Ok(db)
    }

    pub fn asn_reader(&self) -> &Reader<Mmap> {
        &self.asn_reader
    }

    pub fn city_reader(&self) -> &Reader<Mmap> {
        &self.city_reader
    }
}
