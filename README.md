# weewx-data-ingest

A standalone WeeWX 5 service extension that reads data from **external JSON, XML or
plain-text files or web APIs (URLs)** and injects it into the **LOOP packet stream**. From there it's archived
as part of WeeWX's normal archive cycle, like any other sensor reading.

If you have data that WeeWX's driver doesn't supply, it can go into your WeeWX database
without writing a driver or a service of your own. Examples:

* air-quality, pollen or UV readings from a web API, polled within its rate limits;
* a local sensor or gadget that publishes JSON (a particulate monitor, a lightning
  detector, a home-automation hub);
* a space-weather or aurora XML feed;
* values another program writes as simple `key = value` lines.

At startup the service checks the database for a suitable column for every value. If one is
missing, it can **add the column to your main archive**, or **create a separate database**
and add columns there as needed, so your main archive schema stays untouched.

---

## Install

```bash
weectl extension install weewx-data-ingest.zip
```

The installer asks where columns that aren't already in your archive should go:

```
Do you want to add to existing weewx.sdb database or create a new database (existing/new) [new]:
New database name (without .sdb) [dataingest]:
```

* **existing** adds new columns to your main archive (`missing_columns = add`). Back up
  `weewx.sdb` first. The name question is skipped.
* **new** stores them in a separate database (`missing_columns = separate`). Type just the
  name, e.g. `airdata`; `.sdb` is added automatically, giving `airdata.sdb` alongside
  `weewx.sdb`. If you type the suffix anyway, it isn't doubled. The name can't be the same as
  the main archive. If your main archive is MySQL, the new database is MySQL too and no
  suffix is added.

Press Enter to accept the default shown in brackets. The database itself is created the
first time WeeWX starts.

For scripted installs, give the answers as options instead. If there's no input at all, the
defaults (`new`, `dataingest`) are used:

```bash
weectl extension install weewx-data-ingest.zip --database=existing
weectl extension install weewx-data-ingest.zip --database=new --database-name=airdata
```

Then add your sources to `[DataIngest]` in `weewx.conf` (see below) and restart WeeWX.

The installer also:

* adds `user.dataingest.DataIngestService` to the **front** of `data_services`, so any
  other service that uses LOOP packets (MQTT publishers, live-data services) sees the
  injected values. Nothing else in your service lists is changed;
* adds a `[DataIngest]` section and, for **new**, `[DataBindings][[dataingest_binding]]` and
  `[Databases][[dataingest_db]]`. Other settings already in `weewx.conf` are left alone.

Re-installing asks again, and the new answer replaces the old one. A separate database you've
stopped using stays configured, so its history can still be reached in reports.

Uninstall with `weectl extension uninstall dataingest`. Any separate database file is left
in place.

## How it fits into the archive cycle

```
external file ──► DataIngestService ──► LOOP packet ──► StdArchive accumulator ──► archive record
 (JSON/XML/txt)      (data service)          │                                      │
                                             └► other loop consumers                ├► main archive (weewx.sdb)
                                                (MQTT, live data…)                  └► separate DB (<name>.sdb)
```

* **Loop injection** (`inject_loop = true`) adds the values to every LOOP packet. StdArchive
  then accumulates them like any sensor reading: average by default, or as set by the
  source's `extractor` or your `[Accumulator]` section.
* **Archive injection** (`inject_archive = true`) also writes the latest value straight into
  the archive record, overriding the accumulated value. Catch-up records (older than
  `archive_window`, e.g. downloaded from a logger after a restart) are skipped, because the
  file describes *now*, not then.
* The file is re-parsed only when its modification time changes. If a read catches the file
  half-written, the last good copy is used.

A source can be a local file or an `http://` / `https://` URL. URLs are fetched in the
background, never more often than you allow (see
[Reading from a URL](#reading-from-a-url-api-rate-limits)). For a local file, keeping it up
to date is the job of whatever writes it.

## Database columns

At WeeWX startup each mapped column is routed:

| Column state | Result |
|---|---|
| Exists in the main archive and is numeric | Archived there by StdArchive, as normal |
| Missing (or exists but isn't numeric), policy **`separate`** *(default)* | Created in the separate database and stored there every archive period |
| Missing, policy **`add`** | `REAL` column added to the main archive, with its daily summary |
| Missing, policy **`warn`** | Warning logged. The value is in LOOP packets but isn't archived |

Set the policy for everything with `[DataIngest] missing_columns`, or per source. If two
sources map to the same column, the stronger policy wins (`add` > `separate` > `warn`).

WeeWX's standard `wview_extended` schema already has many general-purpose columns: `o3`,
`no2`, `pm2_5`, `pm10_0`, `co`, `lightning_strike_count`, `extraTemp1`…,
`soilTemp1`…, `leafWet1`… Mapping onto those needs no schema change at all.

**The separate database** is an ordinary WeeWX database: an `archive` table
(`dateTime`, `usUnits`, `interval` + your columns) and daily summaries. A row is written
only when the archive record contains at least one of its columns. That write runs *after*
StdArchive, so it also works with hardware record generation. Use it in reports via its
binding:

```
$latest(data_binding='dataingest_binding').auroraActivity
$day(data_binding='dataingest_binding').auroraActivity.max
$week(data_binding='dataingest_binding').auroraActivity.avg
```

and in ImageGenerator plots with `data_binding = dataingest_binding` on the plot or line.

To use MySQL, or a different file or table name, edit `[DataBindings][[dataingest_binding]]`
/ `[Databases]` (or point `separate_binding` at your own binding). Keep
`schema = user.dataingest.schema`.

> **Note:** if you later change a column's policy (say, from `separate` to `add`), new data
> goes to the new place and history stays where it was. Moving it is up to you.

**Adding a column to the main archive** (`add`) changes its schema. Back up `weewx.sdb`
first, as you would before `weectl database add-column`.

## Configuration

```ini
[DataIngest]
    enable = true
    missing_columns = separate      # separate | add | warn
    # data_binding = wx_binding     # main archive
    # separate_binding = dataingest_binding

    # Any source option can also be set here as a default for all sources:
    # max_age = 900

    [[groups]]
        # Unit groups for new columns, so reports convert and label them.
        auroraActivity = group_count

    [[sources]]
        # JSON straight from a web API, polled every 15 minutes and never more
        # than 96 times a day. o3/no2/pm2_5 already exist in the standard
        # schema, so they go straight into the main archive.
        [[[airquality]]]
            path = https://api.example.com/v1/air?lat={latitude}&lon={longitude}
            poll_interval = 900
            max_requests_per_day = 96
            json_path = current
            [[[[headers]]]]
                X-API-Key = your-key-here
            [[[[mapping]]]]
                ozone = o3
                nitrogen_dioxide = no2
                pm2_5 = pm2_5

        # XML feed. New columns -> separate database (the default policy).
        [[[aurora]]]
            path = /var/lib/weewx-feeds/aurora.xml
            json_path = current_status
            extractor = last
            [[[[mapping]]]]
                activity.-1.value = auroraActivity
                site_status.@status_id = auroraStatus
            [[[[text_values]]]]
                green = 0
                yellow = 1
                amber = 2
                red = 3

        # "key = value" lines written by another program, in metric units.
        [[[garden]]]
            path = /home/pi/garden.txt
            unit_system = METRIC
            missing_columns = add
            mapping = soil:soilTemp1, leaf:leafWet1, pot_moisture:potMoisture
```

### Source options

| Option | Default | Meaning |
|---|---|---|
| `path` | *(required)* | File to read, or an `http://`/`https://` URL. `disable` (or `enable = false`) turns the source off |
| `format` | `auto` | `json`, `xml`, `keyvalue` or `auto`. Auto picks by first character: `{`/`[` → JSON, `<` → XML, else key/value |
| `json_path` | — | Dotted path to the part of the file the mapping is relative to (`xml_path` is an alias) |
| `mapping` | *(required)* | `source_path = column` subsection, or `"a:col1, b:col2"` |
| `inject_loop` | `true` | Add values to LOOP packets |
| `inject_archive` | `true` | Also set the latest value directly in archive records |
| `max_age` | `0` (file), 3 × `poll_interval` (URL) | Seconds. Don't inject data older than this: file age, or time since the last successful fetch. `0` = no limit |
| `when` | `always` | `always`, `day` or `night` (sun above/below horizon at the station) |
| `overwrite` | `true` | `false` keeps any value the station already supplied |
| `unit_system` | *(database's)* | Unit system **of the file**: `US`, `METRIC` or `METRICWX`. If not set, the file is taken to be in the database unit system. See [Units](#units) |
| `[[[[units]]]]` | — | For files that mix unit systems: the unit **a value arrives in**, keyed by database column, e.g. `windGust = km_per_hour`. Only tells the service how to read that value; it's still converted to the database's unit for the column and never changes what's stored. See [Units](#units) |
| `archive_window` | `900` | Skip archive records further than this from now (catch-up). `0` = never skip |
| `missing_columns` | *(section)* | Per-source override of the column policy |
| `extractor` | *(WeeWX default)* | How LOOP values become the archive value: `avg`, `last`, `first`, `min`, `max`, `sum`, `count`. Added to `[Accumulator]` unless you have set one there already |
| `[[[[text_values]]]]` | — | Map words to numbers (`green = 0`), case-insensitive |

URL sources also take:

| Option | Default | Meaning |
|---|---|---|
| `poll_interval` | `900` | Seconds between requests. Minimum `60` |
| `max_requests_per_day` | `0` (none) | Hard cap on requests in any rolling 24 hours |
| `retry_interval` | `poll_interval` | Wait after a failed request; doubles with each further failure, up to the larger of `poll_interval` and 1 hour. Minimum `60` |
| `timeout` | `10` | Seconds to wait for the server |
| `[[[[headers]]]]` | — | Extra request headers, e.g. `X-API-Key = ...` or a `User-Agent` some APIs require |
| `[[[[params]]]]` | — | Query parameters added to the URL (URL-encoded for you), e.g. `appid = ...` |
| `method` | `GET`, or `POST` with a form/body | `GET` or `POST` |
| `[[[[form]]]]` | — | Form fields sent as a `POST` body (`application/x-www-form-urlencoded`) |
| `body` | — | Raw `POST` body, e.g. `'{"lat": {latitude}}'`. JSON is detected; `content_type` overrides |
| `[[[[auth]]]]` | — | Basic, bearer token, OAuth2 or a custom token endpoint. See [Keys, tokens and logins](#keys-tokens-and-logins) |
| `save_to` | — | Also write each good response to this file, for other programs |

`[DataIngest] cache_dir` sets where URL responses and request logs are kept. The default is
`dataingest_cache` alongside your databases.

Column names must be letters, digits and `_`. `dateTime`, `usUnits` and `interval` are
refused as targets.

### Reading from a URL (API rate limits)

Many APIs limit how often you may call them, and break the rules too often and your key can
be suspended. The service is built never to exceed the limits you set, **including across
WeeWX restarts and while the API is failing**:

* **`poll_interval`** — a new request is made only once this many seconds have passed since
  the last one. A good response is saved to disk. After a restart (config change, upgrade,
  reboot) the saved response is used straight away, and the next request waits for the
  remainder of the interval. Restarting WeeWX ten times in a row costs no extra requests.
* **`max_requests_per_day`** — a hard cap over any rolling 24 hours. The request log is
  saved, so restarts don't reset it. Nothing, not even `--fetch` on the command line, can
  exceed it.
* **Failures** — a failed request is retried no sooner than `retry_interval` (by default
  `poll_interval`). Each further failure doubles the wait, up to 1 hour or `poll_interval`
  if that's longer. Failed requests still count towards `max_requests_per_day`, because most
  APIs count them too.
* **`429 Too Many Requests` / `503`** with a `Retry-After` header — the server's wait is
  always honoured, even by `--fetch`.
* **Unchanged data** — if the server supports it (ETag / Last-Modified), later requests ask
  "has it changed?" and an unchanged reply is a tiny `304 Not Modified`. These still count as
  requests.
* **Shared URLs** — sources with the same URL and headers share one fetcher, so one request
  serves them all. The strictest settings among them apply.
* **Bad responses** — an error page, an empty reply or anything that doesn't parse never
  replaces the last good data.

Fetching runs in a background thread, so a slow or unreachable API never delays WeeWX. The
command-line helper shares the same saved state and request log as the running service,
so running `test` doesn't add requests beyond your limits either.

Work out the settings from the API's terms. For "1,000 calls per day", for example,
`poll_interval = 900` uses 96, and `max_requests_per_day = 1000` guarantees the cap. Check
how often the API's data actually changes, too: polling a forecast that updates hourly every
minute gains nothing.

URLs, `params`, `form` and `body` can include `{latitude}`, `{longitude}` and `{altitude}`
(metres) from `[Station]`. Only `http://` and `https://` are accepted, and responses over
5 MB are refused.

### Keys, tokens and logins

Credentials don't have to be part of the URL. Put each in the place the API expects it:

```ini
        [[[pollen]]]
            path = https://api.example.com/v2/pollen
            [[[[params]]]]                       # ?lat=..&lon=..&appid=...
                lat = {latitude}
                lon = {longitude}
                appid = env:POLLEN_API_KEY
            [[[[headers]]]]                      # or as a header
                X-API-Key = file:/etc/weewx/secrets/pollen.key
```

**Keeping secrets out of `weewx.conf`:** any value in `params`, `headers`, `form`, `body` or
`auth` can be `env:NAME` (an environment variable) or `file:/path` (the file's contents).
Keep secret files readable only by the WeeWX user (`chmod 600`). When WeeWX runs as a
service, environment variables are set in the service definition, e.g. a systemd
`Environment=` or `EnvironmentFile=` line. If a reference can't be resolved, the source is
disabled with a message that names the variable or file, never the secret.

**`[[[[auth]]]]`** covers logins that go beyond a fixed key:

| `type` | Use for | Options |
|---|---|---|
| `basic` | HTTP Basic login | `username`, `password` |
| `bearer` | A fixed token | `token`; sent as `Authorization: Bearer <token>`, or as a query parameter with `param = api_key` |
| `oauth2` | OAuth2 *client credentials* (a client ID and secret exchanged for a short-lived token) | `token_url`, `client_id`, `client_secret`, optional `scope`; `client_auth = basic` (default, per the standard) or `body` |
| `token` | Any other "log in, get a token" API | `token_url`, optional `method` (default `POST`), `[[[[[form]]]]]`, `body`, `[[[[[headers]]]]]`, `[[[[[params]]]]]`; `token_path` (where the token is in the JSON reply, default `access_token`), `expires_path` (default `expires_in`), `token_lifetime` (seconds, if the reply doesn't say; default 3600) |

For `oauth2` and `token`, the token is sent as `header = Authorization` with
`prefix = Bearer` by default. Use `prefix = none` for a bare token, or `param = name` to send
it as a query parameter instead.

```ini
        [[[oauth_api]]]
            path = https://api.example.com/v1/observations
            [[[[auth]]]]
                type = oauth2
                token_url = https://auth.example.com/oauth/token
                client_id = my-client-id
                client_secret = env:EXAMPLE_CLIENT_SECRET
                scope = read:observations

        [[[login_api]]]
            path = https://device.example.com/api/latest
            [[[[auth]]]]
                type = token
                token_url = https://device.example.com/api/login
                body = file:/etc/weewx/secrets/login.json
                token_path = data.session_token
                header = X-Session
                prefix = none
```

Here `/etc/weewx/secrets/login.json` holds the whole login request, e.g.
`{"username": "weewx", "password": "..."}`, so the password isn't in `weewx.conf`.

Tokens are requested only when there isn't one or it's within a minute of expiring, and they
are saved with the request log, in a file only the WeeWX user can read. A restart doesn't
log in again. If the API rejects a token (401/403), it's dropped and a new one is requested
at the next attempt, which still waits for the normal retry interval. Token requests are
shown separately in the request count and don't use up `max_requests_per_day`.

**Never logged:** API keys, passwords, client secrets and tokens are replaced with `***` in
logs and command-line output. That covers any query parameter, header or form field whose
name looks like a credential (`key`, `token`, `secret`, `auth`, `appid`…), and everything in
`[[[[auth]]]]`. Sources with different credentials never share requests or saved data.

### Units

WeeWX has three unit systems: `US`, `METRIC` and `METRICWX`. They differ in more than
temperature:

| Group | US | METRIC | METRICWX |
|---|---|---|---|
| Temperature | degree_F | degree_C | degree_C |
| Rain | inch | **cm** | **mm** |
| Rain rate | inch_per_hour | cm_per_hour | mm_per_hour |
| Wind speed | mile_per_hour | **km_per_hour** | **meter_per_second** |
| Pressure | inHg | mbar | mbar |

`weewx.conf` uses them in two independent places:

* `[StdReport][[Defaults]] unit_system` sets how data is **emitted** in reports.
  Reports convert stored data to this on output.
* `[StdConvert] target_unit` sets how data is **stored** in the database.

This service is concerned only with storage. Reports then emit ingested data in whatever
unit system they're set to, exactly like station data, as long as the column has a unit
group. That applies to the separate database too.

**Every database column holds exactly one unit.** `unit_system` and `units` only describe the
**incoming file**. You never choose the units data is stored in: archived data is always
written in the **database unit system**, which the service reads from `weewx.conf`:

* `[StdConvert] target_unit` (`US` if not set), as long as `weewx.engine.StdConvert` is in
  `process_services`, as it is by default;
* if the main database already holds data in a different unit system, the database wins and
  a warning is logged. WeeWX itself won't archive in any other system once data exists.

Each value is converted from the file's units to the units of the packet or record it goes
into. WeeWX then converts packets to the database unit system as usual. This matters because
LOOP packets reach data services in the **driver's** units, before `StdConvert`. A METRIC
station with a US database, or the other way round, therefore still gets the right values.
The separate database is written in the same unit system as the main archive.

Conversion needs a unit group for the column. Every standard column already has one; for new
columns, add a `[[groups]]` entry (`grassTemp = group_temperature`). The service refuses
anything that would put a second unit into a column:

* **Can't convert:** if a source says a value arrives in different units from the database
  (through `unit_system` or `units`) but the column has no unit group, the value is **not
  stored**. The service logs why at startup, and `test` shows `NOT STORED`. Add a
  `[[groups]]` entry to fix it.
* **Can't regroup:** `[[groups]]` can't change the group of a column WeeWX already knows,
  e.g. `extraTemp1 = group_speed`. New values would be converted differently from the data
  already in the column, so the entry is ignored with an error.
* **Impossible conversion:** a value whose unit can't be converted to the column's group
  (e.g. `km_per_hour` for a temperature) is dropped.

Two cases need no conversion at all. Quantities that use the same unit in every system
(percent, µg/m³, UV index, counts…) are never affected by `unit_system`. A column with no
unit group and no declared units is taken to be in the database's units already, and stored
as read.

Unit names are WeeWX's own: `degree_C`, `degree_F`, `km_per_hour`,
`meter_per_second`, `mile_per_hour`, `knot`, `hPa`, `mbar`, `inHg`, `mm`, `inch`,
`microgram_per_meter_cubed`, `percent`, `watt_per_meter_squared` and so on.

Pick the file's `unit_system` by checking **every** mapped quantity, not just temperature.
A typical "metric" web API gives rain in mm and wind in km/h. That's neither METRIC (rain in
cm) nor METRICWX (wind in m/s), so describe the file with one system, plus `units` for the odd
one out. With a US database, all three values below are stored in US units (°F, inches,
mph), the same as the rest of each column:

```ini
        [[[mixed]]]
            path = /var/lib/weewx-feeds/mixed.json
            unit_system = METRICWX          # °C, rain in mm, hPa...
            mapping = temp:extraTemp1, gust:windGust, rain:rain
            [[[[units]]]]
                windGust = km_per_hour      # ...but wind in km/h, not m/s
```

The `test` command prints the database unit system, the file's assumed units, and each
value as it will be stored: converted, or `NOT STORED` with the reason.

### Paths into JSON, XML and text

The same dotted paths work for every format:

* `current.temp` – nested keys
* `hourly.temp.0`, `entries.-1.value` – list index; `-1` is the last
* **XML:** the root element's name is the first step. Attributes are `@name`; an element
  that has attributes keeps its text as `#text` (used automatically when mapped). Repeated
  elements become lists. Namespace prefixes are dropped (`georss:point` → `point`). Index `0`
  or `-1` also works on an element that appears only once, so the path doesn't break when a
  feed has a single entry.
* **Text:** one `key = value`, `key: value` or `key,value` per line. Lines starting `#` or
  `;` are comments.

Non-numeric values (`null`, `"N/A"`, words not in `text_values`) are skipped.

If `defusedxml` is installed, it is used for XML parsing.

Not sure what path to use? Run the `paths` command below on the file.

## Command-line helper

Run with WeeWX's `bin` directory (the one containing `user/`) on `PYTHONPATH`, e.g. for a
pip install `PYTHONPATH=~/weewx-data/bin`, for a package install `PYTHONPATH=/etc/weewx/bin`:

```bash
# Read every source: values found, and where each column will be archived
python3 -m user.dataingest --config /etc/weewx/weewx.conf test

# List every path in a source's file or URL (or any file or URL) -- for writing mappings
python3 -m user.dataingest --config /etc/weewx/weewx.conf paths aurora
python3 -m user.dataingest --config /etc/weewx/weewx.conf paths /tmp/some-feed.xml

# URL sources: test and paths use the saved response unless a request is due, and
# show the request count and when the next one is allowed. --fetch requests now,
# within max_requests_per_day and any Retry-After.
python3 -m user.dataingest --config /etc/weewx/weewx.conf test --fetch

# Create missing columns now instead of at the next WeeWX start
# (stop WeeWX first if this touches the main archive)
python3 -m user.dataingest --config /etc/weewx/weewx.conf setup-columns
```

## Using it alongside other extensions

`weewx-data-ingest` is independent: it reads only its own `[DataIngest]` section and never
removes or reconfigures other services. If another extension (a skin's own data service,
say) already injects the same value into the same column, configure only one of them for
that value. Otherwise the value is injected twice, and the last service to run wins.

## Requirements

WeeWX 5.x (tested on 5.5.2), Python 3.7+.

## Licence

GPL v3. See `LICENSE`.
