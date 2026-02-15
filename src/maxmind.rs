#![allow(clippy::unused_unit)]
use maxminddb::{geoip2, Mmap, Reader};
use polars::prelude::*;
use std::env;
use std::io;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

// Define the fields and types that we will support
pub const MAXMIND_FIELDS: [(&str, DataType); 12] = [
    ("asnnum", DataType::UInt32),
    ("asnorg", DataType::String),
    ("city", DataType::String),
    ("continent", DataType::String),
    ("country", DataType::String),
    ("country_iso", DataType::String),
    ("latitude", DataType::Float64),
    ("longitude", DataType::Float64),
    ("postalcode", DataType::String),
    ("subdivision", DataType::String),
    ("subdivision_iso", DataType::String),
    ("timezone", DataType::String),
];

// Define a struct to hold all the fields using &str instead of String
#[derive(Debug)]
pub struct MaxmindIPResult<'a> {
    pub asnnum: u32,
    pub asnorg: &'a str,
    pub city: &'a str,
    pub continent: &'a str,
    pub country: &'a str,
    pub country_iso: &'a str,
    pub latitude: f64,
    pub longitude: f64,
    pub postalcode: &'a str,
    pub subdivision: &'a str,
    pub subdivision_iso: &'a str,
    pub timezone: &'a str,
}

impl Default for MaxmindIPResult<'_> {
    fn default() -> Self {
        Self {
            asnnum: 0,
            asnorg: "",
            city: "",
            continent: "",
            country: "",
            country_iso: "",
            latitude: 0.0,
            longitude: 0.0,
            postalcode: "",
            subdivision: "",
            subdivision_iso: "",
            timezone: "",
        }
    }
}

// Modern Rust 2021+ approach using OnceLock instead of lazy_static
// This instantiates a lazily loaded global connection to MaxMind
// mmdb database files for re-use
static MAXMIND_DB: OnceLock<Mutex<Result<MaxMindDB, PolarsError>>> = OnceLock::new();

/// Object to hold connections to ASN and City MaxMind MMDB readers
#[derive(Debug)]
pub struct MaxMindDB {
    asn_reader: Reader<Mmap>,
    city_reader: Reader<Mmap>,
}

/// Helper function to locate the MaxMind MMDB directory on the system
/// deferring foremost to the environment variable `MAXMIND_MMDB_DIR` and
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
    /// found or mmdb files could not be opened, raise a `PolarsCompute`
    /// error so it propagates back up to the python user
    fn initialize() -> PolarsResult<Self> {
        let mmdb_dir = get_mmdb_dir().map_err(|_| {
            PolarsError::ComputeError(
                "Error could not locate a directory for MaxMind MMDB files\n\
                 Hint: specify a directory with the environment variable MAXMIND_MMDB_DIR\n"
                    .into(),
            )
        })?;

        let asn_path = Path::new(&mmdb_dir).join("GeoLite2-ASN.mmdb");
        let city_path = Path::new(&mmdb_dir).join("GeoLite2-City.mmdb");

        let asn_reader = Reader::open_mmap(&asn_path).map_err(|e| {
            PolarsError::ComputeError(
                format!(
                    "Could not open ASN MMDB file from {}: {}",
                    asn_path.to_str().unwrap_or_default(),
                    e
                )
                .into(),
            )
        })?;

        let city_reader = Reader::open_mmap(&city_path).map_err(|e| {
            PolarsError::ComputeError(
                format!(
                    "Could not open City MMDB file from {}: {}",
                    city_path.to_str().unwrap_or_default(),
                    e
                )
                .into(),
            )
        })?;

        Ok(Self {
            asn_reader,
            city_reader,
        })
    }

    /// Force a reinitialization of the MMDB readers by dropping
    /// the existing global reader and invoking `initialize()` again.
    /// This is helpful, particularly in an interactive session (e.g., Jupyter)
    /// and the user has changed `MAXMIND_MMDB_DIR` setting or updated
    /// the MaxMind mmdb files themselves
    pub fn reload() -> PolarsResult<()> {
        let db = MAXMIND_DB.get_or_init(|| Mutex::new(Self::initialize()));
        let mut guard = db
            .lock()
            .map_err(|_| PolarsError::ComputeError("Failed to acquire MaxMindDB lock".into()))?;
        *guard = Self::initialize();
        Ok(())
    }

    /// Gets the global mmdb reader, initializing it first if necessary
    pub fn get_or_init() -> PolarsResult<std::sync::MutexGuard<'static, Result<Self, PolarsError>>>
    {
        let db = MAXMIND_DB.get_or_init(|| Mutex::new(Self::initialize()));
        db.lock()
            .map_err(|_| PolarsError::ComputeError("Failed to acquire MaxMindDB lock".into()))
    }

    pub fn asn_reader(&self) -> &Reader<Mmap> {
        &self.asn_reader
    }

    // pub fn city_reader(&self) -> &Reader<Mmap> {
    //     &self.city_reader
    // }

    pub fn iplookup(&self, ip: IpAddr) -> MaxmindIPResult<'_> {
        let mut result = MaxmindIPResult::default();

        // Lookup ASN information
        if let Ok(asn) = self.asn_reader.lookup::<geoip2::Asn>(ip) {
            result.asnnum = asn.autonomous_system_number.unwrap_or(0);
            result.asnorg = asn.autonomous_system_organization.unwrap_or("");
        }

        // Lookup City information
        if let Ok(city_result) = self.city_reader.lookup::<geoip2::City>(ip) {
            // as_ref() and &**s magic provided by ChatGPT on 20240825 using GPT-4o
            result.city = city_result
                .city
                .as_ref()
                .and_then(|city| {
                    city.names
                        .as_ref()
                        .and_then(|names| names.get("en").map(|s| &**s))
                })
                .unwrap_or("");

            result.continent = city_result
                .continent
                .as_ref()
                .and_then(|continent| {
                    continent
                        .names
                        .as_ref()
                        .and_then(|names| names.get("en").map(|s| &**s))
                })
                .unwrap_or("");

            result.country = city_result
                .country
                .as_ref()
                .and_then(|country| {
                    country
                        .names
                        .as_ref()
                        .and_then(|names| names.get("en").map(|s| &**s))
                })
                .unwrap_or("");

            result.country_iso = city_result
                .country
                .as_ref()
                .and_then(|country| country.iso_code)
                .unwrap_or("");

            result.latitude = city_result
                .location
                .as_ref()
                .and_then(|loc| loc.latitude)
                .unwrap_or(0.0);

            result.longitude = city_result
                .location
                .as_ref()
                .and_then(|loc| loc.longitude)
                .unwrap_or(0.0);

            result.postalcode = city_result
                .postal
                .as_ref()
                .and_then(|postal| postal.code)
                .unwrap_or("");

            result.subdivision = city_result
                .subdivisions
                .as_ref()
                .and_then(|subs| {
                    subs.first().and_then(|sub| {
                        sub.names
                            .as_ref()
                            .and_then(|names| names.get("en").map(|s| &**s))
                    })
                })
                .unwrap_or("");

            result.subdivision_iso = city_result
                .subdivisions
                .as_ref()
                .and_then(|subs| subs.first().and_then(|sub| sub.iso_code))
                .unwrap_or("");

            result.timezone = city_result
                .location
                .as_ref()
                .and_then(|loc| loc.time_zone)
                .unwrap_or("");
        }

        result
    }
}
