# Changelog

## 1.0.0 — 2026-10-21

First release.

* Sources: JSON, XML (attributes, repeated elements, namespaces) and key/value text, with dotted paths.
* Values injected into LOOP packets and optionally archive records. Catch-up records skipped.
* Startup column check: numeric column in main archive → used as-is. Missing → `separate`
  (auto-created/extended separate database with daily summaries), `add` (column added to main
  archive) or `warn`.
* Per-source `max_age`, `when` (day/night), `overwrite`, `extractor`, `text_values`.
* Units: `unit_system` and per-column `units` describe the incoming file only. Values are
  always converted to the database unit system, read from `[StdConvert] target_unit` and
  confirmed against existing data; correct whether loop packets arrive in driver or database
  units. A column never mixes units: values that can't be converted (no unit group) are
  refused, and `[[groups]]` can't regroup a column WeeWX already knows.
* URL sources (http/https), fetched in the background within API limits: `poll_interval`
  (min 60 s), `max_requests_per_day`, `retry_interval` with doubling backoff, `Retry-After`
  honoured, conditional requests, shared fetcher for identical URLs. Responses and request
  log are saved, so restarts don't add requests. Credentials are redacted in logs.
* Request details apart from the URL: `[[[[params]]]]`, `[[[[headers]]]]`, `method`, `[[[[form]]]]`,
  `body`; `[[[[auth]]]]` basic / bearer / OAuth2 client credentials / custom token endpoint,
  with tokens cached until expiry; `env:` and `file:` secret references.
* `[[groups]]` to assign unit groups to new columns.
* Installer asks: add to the existing database, or create a new one (name without `.sdb`,
  suffix added automatically). Unattended: `--database=existing|new --database-name=NAME`.
* CLI: `test`, `paths`, `setup-columns`.
