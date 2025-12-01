# Environment Configuration

## MaxMind

IPTools uses two MaxMind databases: _GeoLite2-ASN.mmdb_ and _GeoLite2-City.mmdb_. You only need these files if you call the geoip functions.

### Obtaining the files

The recommended way to keep these files up to date is using the `geoipupdate` tool ([official docs](https://dev.maxmind.com/geoip/updating-databases/#using-geoip-update)).

1.  **Install `geoipupdate`**:
    *   macOS: `brew install geoipupdate`
    *   Linux: Use your package manager (e.g., `apt install geoipupdate`) or download from [GitHub Releases](https://github.com/maxmind/geoipupdate/releases).
2.  **Configure**:
    *   Create a `GeoIP.conf` file (usually in `/usr/local/etc/` or `/etc/`).
    *   Add your `AccountID`, `LicenseKey`, and `EditionIDs` (e.g., `GeoLite2-ASN GeoLite2-City`).
3.  **Run**:
    *   Execute `geoipupdate` to download the files.

### Configuration

Set the `MAXMIND_MMDB_DIR` environment variable to tell the extension where these files are located.

```cmd
export MAXMIND_MMDB_DIR=/path/to/your/mmdb/files
# or Windows users
set MAXMIND_MMDB_DIR=c:\path\to\your\mmdb\files
```

If the environment is not set, polars_iptools will check two other common locations (on Mac/Linux):

```
/usr/local/share/GeoIP
/opt/homebrew/var/GeoIP
```

## Spur

If you're a Spur customer, you can use their anonymous feed in MMDB format.

### Obtaining the file

You can download the anonymous feed as an MMDB file using the Spur Exports API ([official docs](https://docs.spur.us/feeds/exports-api#download-the-anonymous-feed-as-mmdb)):

```bash
curl --get "https://exports.spur.us/v1/feeds/anonymous" \
  --data-urlencode "output=mmdb" \
  -H "Token: $SPUR_TOKEN" \
  -o spur.mmdb
```

### Configuration

Export the feed as `spur.mmdb` and specify its location using `SPUR_MMDB_DIR` environment variable.

```cmd
export SPUR_MMDB_DIR=/path/to/spur/mmdb
# or Windows users
set SPUR_MMDB_DIR=c:\path\to\spur\mmdb
```
