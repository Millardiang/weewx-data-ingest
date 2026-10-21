#
#    weewx-data-ingest
#
#    Inject values from external JSON, XML or plain-text files into WeeWX
#    loop packets and archive records, so they are archived as part of the
#    normal archive cycle.
#
#    Copyright (C) 2025-2026 Ian Millard
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
"""DataIngestService -- a WeeWX data service that reads external files or
http(s) URLs (JSON, XML, or "key = value" text) and injects mapped values
into loop packets and/or archive records. URLs are fetched in the
background within poll_interval / max_requests_per_day, kept across
restarts.

At startup every mapped column is checked against the main archive:

* it exists and is numeric        -> archived there by StdArchive as normal
* it is missing (or not numeric)  -> handled by ``missing_columns``:
    separate  create/extend a separate database (``dataingest_binding``,
              ``dataingest.sdb`` by default) and store those columns there
              every archive period (the default -- the main archive schema
              is never altered)
    add       add a REAL column to the main archive
    warn      log a warning; the value is in loop packets but not archived

Minimal configuration::

    [DataIngest]
        [[sources]]
            [[[airquality]]]
                path = /var/lib/weewx-feeds/airquality.json
                json_path = current
                max_age = 3600
                [[[[mapping]]]]
                    ozone = o3
                    nitrogen_dioxide = no2

Every option is described in README.md. Command-line helper (run with
WeeWX's bin directory on PYTHONPATH)::

    python3 -m user.dataingest --config /path/to/weewx.conf test
    python3 -m user.dataingest --config /path/to/weewx.conf paths SOURCE|FILE
    python3 -m user.dataingest --config /path/to/weewx.conf setup-columns
"""

import json
import logging
import os
import re
import time

import weewx
import weewx.units
from weewx.engine import StdService
from weeutil.weeutil import to_bool, to_int, timestamp_to_string

log = logging.getLogger(__name__)

VERSION = "1.0.0"
SECTION = 'DataIngest'

FORMATS = ('auto', 'json', 'xml', 'keyvalue')
WHEN_CHOICES = ('always', 'day', 'night')
MISSING_CHOICES = ('separate', 'add', 'warn')
EXTRACTORS = ('avg', 'last', 'first', 'min', 'max', 'sum', 'count', 'noop')

MAIN_BINDING = 'wx_binding'
SEPARATE_BINDING = 'dataingest_binding'
SEPARATE_DATABASE = 'dataingest_db'

# Columns WeeWX owns. Never a mapping target.
RESERVED = ('dateTime', 'usUnits', 'interval')
COLUMN_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
NON_NUMERIC_SQL = ('STR', 'CHAR', 'VARCHAR', 'TEXT', 'BLOB', 'DATE', 'TIME')

# Schema for the separate database. Only used when it is first created:
# data columns (and their daily summaries) are added as sources need them.
schema = {
    'table': [('dateTime', 'INTEGER NOT NULL UNIQUE PRIMARY KEY'),
              ('usUnits', 'INTEGER NOT NULL'),
              ('interval', 'INTEGER NOT NULL')],
    'day_summaries': [],
}

DEFAULT_SEPARATE_BINDING = {
    'database': SEPARATE_DATABASE,
    'table_name': 'archive',
    'manager': 'weewx.manager.DaySummaryManager',
    'schema': 'user.dataingest.schema',
}
DEFAULT_SEPARATE_DATABASE = {
    'database_name': 'dataingest.sdb',
    'database_type': 'SQLite',
}

# An archive record whose timestamp is further than this from "now" is a
# catch-up record (e.g. downloaded from the logger's memory after a
# restart). The external file describes *now*, not that moment, so it is
# not injected unless the source sets archive_window = 0.
DEFAULT_ARCHIVE_WINDOW = 900

# URL sources.
DEFAULT_POLL_INTERVAL = 900      # seconds between requests
MIN_POLL_INTERVAL = 60           # floor for poll_interval and retry_interval
DEFAULT_TIMEOUT = 10             # seconds per request
URL_MAX_AGE_POLLS = 3            # default max_age = this many poll intervals
MAX_RESPONSE_BYTES = 5 * 1024 * 1024


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

class SourceConfig(object):
    """One external data source, resolved from weewx.conf."""

    def __init__(self, name, path, mapping, json_path='', fmt='auto',
                 enable=True, inject_loop=True, inject_archive=True,
                 max_age=0, when='always', overwrite=True, unit_system=None,
                 archive_window=DEFAULT_ARCHIVE_WINDOW, missing_columns=None,
                 extractor=None, text_values=None, units=None,
                 poll_interval=DEFAULT_POLL_INTERVAL, timeout=DEFAULT_TIMEOUT,
                 headers=None, save_to=None, retry_interval=None,
                 max_requests_per_day=0, params=None, method=None, form=None,
                 body=None, content_type=None, auth=None):
        self.name = name
        self.path = path
        self.mapping = mapping            # {source key/dotted path: db column}
        self.json_path = json_path        # also used for XML
        self.format = fmt
        self.enable = enable
        self.inject_loop = inject_loop
        self.inject_archive = inject_archive
        self.max_age = max_age            # seconds, 0 = no limit
        self.when = when
        self.overwrite = overwrite
        # Units OF THE INCOMING FILE only -- never what is stored, which is
        # always the database's unit for the column. unit_system None = the
        # file is already in the database's unit system. units = the unit a
        # particular value arrives in, for files that mix systems.
        self.unit_system = unit_system
        self.units = units or {}          # {db column: 'degree_C', ...}
        self.archive_window = archive_window
        self.missing_columns = missing_columns   # None = service default
        self.extractor = extractor        # accumulator extractor, None = WeeWX default
        self.text_values = text_values or {}     # {'green': 0.0, ...} lower-cased
        # URL sources only (path = http://... or https://...).
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.headers = headers or {}      # e.g. {'X-API-Key': '...'}
        self.save_to = save_to            # optional local copy of each response
        self.retry_interval = retry_interval      # after a failure; None = poll_interval
        # Request details kept apart from the URL (values may be secrets).
        self.params = params or {}        # added to the query string
        self.method = method              # None = GET, or POST if there is a form/body
        self.form = form or {}            # x-www-form-urlencoded request body
        self.body = body                  # raw request body (e.g. JSON)
        self.content_type = content_type
        self.auth = auth                  # dict from parse_auth(), or None
        self.max_requests_per_day = max_requests_per_day   # 0 = no daily cap

    @property
    def is_url(self):
        return is_url(self.path)

    @property
    def columns(self):
        return list(self.mapping.values())

    def __repr__(self):
        return "SourceConfig(%s, %s, %d fields)" % (self.name, self.path, len(self.mapping))


def parse_mapping(raw, source_name='?'):
    """Accept a subsection, a "key:col, key:col" string (pipes also
    allowed as separators), or a list of "key:col"."""
    mapping = {}
    if raw is None:
        return mapping
    if isinstance(raw, dict):
        for key, col in raw.items():
            if isinstance(col, dict):
                log.warning("Source '%s': mapping entry '%s' is a section, ignored",
                            source_name, key)
                continue
            mapping[str(key).strip()] = str(col).strip()
        return mapping
    if isinstance(raw, str):
        # configobj only turns comma-separated values into lists; a single
        # value arrives as a string. Pipes were also accepted historically.
        items = raw.replace('|', ',').split(',')
    else:
        items = list(raw)
    for item in items:
        if isinstance(item, dict):
            mapping.update({str(k).strip(): str(v).strip() for k, v in item.items()})
            continue
        item = str(item).strip()
        if not item:
            continue
        if ':' not in item:
            log.warning("Source '%s': mapping entry '%s' has no ':' -- ignored",
                        source_name, item)
            continue
        key, col = item.rsplit(':', 1)
        mapping[key.strip()] = col.strip()
    return mapping


def validate_mapping(mapping, source_name):
    """Drop entries whose target is not usable as a database column."""
    good = {}
    for key, col in mapping.items():
        if col in RESERVED:
            log.error("Source '%s': cannot map '%s' to reserved column '%s' -- ignored",
                      source_name, key, col)
        elif not COLUMN_RE.match(col):
            log.error("Source '%s': '%s' is not a valid column name (letters, digits, _) "
                      "-- ignored", source_name, col)
        else:
            good[key] = col
    return good


def parse_text_values(raw, source_name='?'):
    """[[[[text_values]]]] green = 0 ... -> {'green': 0.0}."""
    out = {}
    if not isinstance(raw, dict):
        return out
    for text, number in raw.items():
        try:
            out[str(text).strip().lower()] = float(number)
        except (TypeError, ValueError):
            log.error("Source '%s': text_values '%s = %s' is not a number -- ignored",
                      source_name, text, number)
    return out


def known_unit(unit):
    """Is this a WeeWX unit name? (Taken from the conversion table and
    the three standard unit systems, which are populated at import time.)"""
    known = set(weewx.units.conversionDict)
    for targets in weewx.units.conversionDict.values():
        known.update(targets)
    for system in (weewx.units.USUnits, weewx.units.MetricUnits, weewx.units.MetricWXUnits):
        known.update(system.values())
    return unit in known


def parse_units(raw, mapping, source_name='?'):
    """[[[[units]]]] column = unit_name  (degree_C, km_per_hour, hPa ...).
    Keyed by DATABASE column, like the right-hand side of the mapping."""
    out = {}
    if not isinstance(raw, dict):
        return out
    columns = set(mapping.values())
    for col, unit in raw.items():
        if isinstance(unit, dict):
            continue
        unit = str(unit).strip()
        if col not in columns:
            log.error("Source '%s': units entry '%s' is not a mapped column -- ignored "
                      "(use the database column name)", source_name, col)
        elif not known_unit(unit):
            log.error("Source '%s': unknown unit '%s' for %s -- ignored (use WeeWX unit "
                      "names such as degree_C, km_per_hour, hPa, mm)", source_name, unit, col)
        else:
            out[col] = unit
    return out


def _unit_system(value, source_name):
    if value in (None, ''):
        return None
    try:
        return weewx.units.unit_constants[str(value).upper()]
    except KeyError:
        log.error("Source '%s': unknown unit_system '%s' (use US, METRIC or "
                  "METRICWX) -- values will be injected unconverted", source_name, value)
        return None


def _choice(value, choices, default, what, source_name):
    if value in (None, ''):
        return default
    value = str(value).lower()
    if value not in choices:
        log.error("Source '%s': unknown %s '%s', using '%s'", source_name, what, value,
                  default)
        return default
    return value


class SecretError(ValueError):
    """A secret reference (env:/file:) could not be resolved. The message
    never contains the secret itself."""


def resolve_secret(value):
    """'env:NAME' -> $NAME, 'file:/path' -> the file's contents (stripped),
    anything else -> itself. Keeps API keys out of weewx.conf."""
    value = str(value)
    if value.startswith('env:'):
        var = value[4:].strip()
        if var not in os.environ:
            raise SecretError("environment variable %s is not set (for WeeWX running as a "
                              "service, set it in the service's environment)" % var)
        return os.environ[var]
    if value.startswith('file:'):
        path = os.path.expanduser(value[5:].strip())
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return f.read().strip()
        except OSError as e:
            raise SecretError("cannot read secret file %s: %s" % (path, e.strerror))
    return value


def string_section(raw, what='?'):
    """A subsection of name = value strings, secrets resolved."""
    out = {}
    if not isinstance(raw, dict):
        return out
    for key, value in raw.items():
        if isinstance(value, dict):
            continue
        if isinstance(value, (list, tuple)):     # configobj splits on commas
            value = ', '.join(str(v) for v in value)
        out[str(key)] = resolve_secret(value)
    return out


AUTH_TYPES = ('basic', 'bearer', 'oauth2', 'token')


def parse_auth(raw):
    """[[[[auth]]]] -> normalised dict, or None. Raises ValueError."""
    if not isinstance(raw, dict) or not raw:
        return None
    a = string_section(raw)
    for sub in ('form', 'headers', 'params'):
        a[sub] = string_section(raw.get(sub))
    kind = a.get('type', '').lower()
    if kind not in AUTH_TYPES:
        raise ValueError("[[[[auth]]]] type must be one of %s" % ', '.join(AUTH_TYPES))
    a['type'] = kind
    need = {'basic': ('username', 'password'), 'bearer': ('token',),
            'oauth2': ('token_url', 'client_id', 'client_secret'),
            'token': ('token_url',)}[kind]
    missing = [k for k in need if not a.get(k)]
    if missing:
        raise ValueError("[[[[auth]]]] type = %s needs %s" % (kind, ', '.join(missing)))
    for key in ('token_url',):
        if a.get(key) and not is_url(a[key]):
            raise ValueError("[[[[auth]]]] token_url must be http:// or https://")
    a.setdefault('token_path', 'access_token')
    a.setdefault('expires_path', 'expires_in')
    a['token_lifetime'] = to_int(a.get('token_lifetime', 3600))
    a.setdefault('header', 'Authorization')
    # Header value is "<prefix> <token>"; prefix = none sends the bare token.
    a['prefix'] = a.get('prefix', 'Bearer').strip()
    if a['prefix'].lower() == 'none':
        a['prefix'] = ''
    a['client_auth'] = a.get('client_auth', 'basic').lower()
    if a['client_auth'] not in ('basic', 'body'):
        raise ValueError("[[[[auth]]]] client_auth must be basic or body")
    return a


def _request_options(sec, name):
    """The request-detail options of a URL source. Raises ValueError."""
    method = str(sec.get('method', '')).upper() or None
    if method not in (None, 'GET', 'POST'):
        raise ValueError("method must be GET or POST")
    body = sec.get('body')
    if isinstance(body, (list, tuple)):
        body = ', '.join(str(b) for b in body)
    return dict(
        headers=string_section(sec.get('headers')),
        params=string_section(sec.get('params')),
        form=string_section(sec.get('form')),
        body=resolve_secret(body) if body not in (None, '') else None,
        content_type=sec.get('content_type'),
        method=method,
        auth=parse_auth(sec.get('auth')),
    )


def _source_from_section(name, sec, defaults):
    def opt(key, fallback):
        return sec.get(key, defaults.get(key, fallback))

    path = str(sec.get('path', '')).strip()
    if re.match(r'^[A-Za-z][A-Za-z0-9+.-]*://', path) and not is_url(path):
        log.error("Source '%s': only http:// and https:// URLs are supported -- disabled",
                  name)
        path = ''
    mapping = validate_mapping(parse_mapping(sec.get('mapping'), name), name)
    url = is_url(path)
    poll = to_int(opt('poll_interval', DEFAULT_POLL_INTERVAL))
    if url and poll < MIN_POLL_INTERVAL:
        log.warning("Source '%s': poll_interval %d s is below the minimum; using %d s",
                    name, poll, MIN_POLL_INTERVAL)
    poll = max(MIN_POLL_INTERVAL, poll)
    retry = opt('retry_interval', None)
    retry = max(MIN_POLL_INTERVAL, to_int(retry)) if retry not in (None, '') else None
    request = {}
    if url:
        try:
            request = _request_options(sec, name)
        except ValueError as e:
            log.error("Source '%s': %s -- disabled", name, e)
            path = ''
    max_age = opt('max_age', None)
    if max_age in (None, ''):
        # A URL source's last response must not be injected for ever if
        # the API goes away; a local file is the user's business.
        max_age = URL_MAX_AGE_POLLS * poll if url else 0
    return SourceConfig(
        name=name,
        path=path,
        mapping=mapping,
        json_path=str(sec.get('json_path', sec.get('xml_path', ''))).strip(),
        fmt=_choice(opt('format', 'auto'), FORMATS, 'auto', 'format', name),
        enable=to_bool(sec.get('enable', True)) and bool(path) and path.lower() != 'disable',
        inject_loop=to_bool(opt('inject_loop', True)),
        inject_archive=to_bool(opt('inject_archive', True)),
        max_age=to_int(max_age),
        when=_choice(opt('when', 'always'), WHEN_CHOICES, 'always', 'when', name),
        overwrite=to_bool(opt('overwrite', True)),
        unit_system=_unit_system(opt('unit_system', None), name),
        archive_window=to_int(opt('archive_window', DEFAULT_ARCHIVE_WINDOW)),
        # Per-source override only; the section-wide default is applied
        # by the service so it is not copied into every source here.
        missing_columns=_choice(sec.get('missing_columns'), MISSING_CHOICES, None,
                                'missing_columns', name),
        extractor=_choice(sec.get('extractor'), EXTRACTORS, None, 'extractor', name),
        text_values=parse_text_values(sec.get('text_values'), name),
        units=parse_units(sec.get('units'), mapping, name),
        poll_interval=poll,
        retry_interval=retry,
        max_requests_per_day=max(0, to_int(opt('max_requests_per_day', 0))),
        timeout=max(1, to_int(opt('timeout', DEFAULT_TIMEOUT))),
        save_to=str(sec['save_to']).strip() if sec.get('save_to') else None,
        **request
    )


def service_section(config_dict):
    """The [DataIngest] section, or {} if there is none."""
    section = config_dict.get(SECTION)
    return section if isinstance(section, dict) else {}


def load_sources(config_dict):
    """All sources in [DataIngest][[sources]]. Options set directly in
    [DataIngest] are defaults for every source."""
    section = service_section(config_dict)
    defaults = {k: v for k, v in section.items() if not isinstance(v, dict)}
    sources = []
    for name, sec in (section.get('sources', {}) or {}).items():
        if isinstance(sec, dict):
            sources.append(_source_from_section(name, sec, defaults))
    return sources


# ---------------------------------------------------------------------------
# Reading files
# ---------------------------------------------------------------------------

def parse_keyvalue(text):
    """'key = value', 'key: value' or 'key,value' per line; '#' comments."""
    data = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith(('#', ';')):
            continue
        for sep in ('=', ':', ','):
            if sep in line:
                key, value = line.split(sep, 1)
                data[key.strip()] = value.strip().strip('"\'')
                break
    return data


def _strip_ns(name):
    """'{http://www.w3.org/2005/Atom}entry' -> 'entry'; 'georss:point' -> 'point'."""
    return name.rsplit('}', 1)[-1].rsplit(':', 1)[-1]


def _xml_node(elem):
    children = list(elem)
    attrs = {'@' + _strip_ns(k): v for k, v in elem.attrib.items()}
    text = (elem.text or '').strip()
    if not children and not attrs:
        return text
    node = dict(attrs)
    repeated = set()
    for child in children:
        tag = _strip_ns(child.tag)
        value = _xml_node(child)
        if tag not in node:
            node[tag] = value
        else:
            # Repeated element -> list, in document order.
            if tag not in repeated:
                node[tag] = [node[tag]]
                repeated.add(tag)
            node[tag].append(value)
    if text:
        node['#text'] = text
    return node


def parse_xml(text):
    """XML -> nested dicts/lists, wrapped in the root element's name.

        <status updated="..."><site id="SUM">green</site></status>
     -> {'status': {'@updated': '...', 'site': {'@id': 'SUM', '#text': 'green'}}}

    Attributes become '@name', an element with attributes or children
    keeps its own text as '#text', repeated elements become lists and
    namespace prefixes are dropped."""
    try:
        from defusedxml import ElementTree as ET   # preferred when installed
    except ImportError:
        import xml.etree.ElementTree as ET
    if isinstance(text, str):
        # ElementTree refuses str input that carries an encoding declaration.
        text = text.encode('utf-8')
    try:
        root = ET.fromstring(text)
    except ET.ParseError as e:
        raise ValueError("XML parse error: %s" % e)
    return {_strip_ns(root.tag): _xml_node(root)}


def parse_text(text, fmt='auto'):
    """Parse file contents. 'auto' looks at the first character: '{' or
    '[' is JSON (whatever the file extension -- many feeds save JSON as .txt),
    '<' is XML, anything else key/value lines."""
    if fmt == 'json':
        return json.loads(text)
    if fmt == 'xml':
        return parse_xml(text)
    if fmt == 'keyvalue':
        return parse_keyvalue(text)
    stripped = text.lstrip('﻿ \t\r\n')
    if stripped.startswith(('{', '[')):
        return json.loads(stripped)
    if stripped.startswith('<'):
        return parse_xml(stripped)
    return parse_keyvalue(text)


def lookup(data, path):
    """Follow a dotted path through dicts and lists ('hourly.temp.0',
    'feed.entry.-1.title', 'status.site.@id'). Returns (found, value).

    An index of 0 or -1 on a single (non-list) element selects that
    element, because XML only produces a list once an element repeats."""
    if not path:
        return True, data
    node = data
    for part in str(path).split('.'):
        if part == '':
            continue
        if isinstance(node, dict):
            if part in node:
                node = node[part]
            elif part in ('0', '-1'):
                continue
            else:
                return False, None
        elif isinstance(node, list):
            try:
                node = node[int(part)]
            except (ValueError, IndexError):
                return False, None
        else:
            return False, None
    return True, node


def flatten(data, prefix=''):
    """Yield (path, value) for every leaf -- used by the 'paths' command."""
    if isinstance(data, dict):
        for key, value in data.items():
            for item in flatten(value, '%s.%s' % (prefix, key) if prefix else str(key)):
                yield item
    elif isinstance(data, list):
        for i, value in enumerate(data):
            for item in flatten(value, '%s.%d' % (prefix, i) if prefix else str(i)):
                yield item
    else:
        yield prefix, data


def to_number(value, text_values=None):
    """float() for numbers and numeric strings; text_values for known
    words; None for anything else (null, '', 'N/A', lists...)."""
    if isinstance(value, dict):
        # XML element with attributes: use its text.
        if '#text' not in value:
            return None
        value = value['#text']
    if value is None or isinstance(value, list):
        return None
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    try:
        number = float(value)
    except (TypeError, ValueError):
        if text_values and isinstance(value, str):
            return text_values.get(value.strip().lower())
        return None
    if number != number or number in (float('inf'), float('-inf')):
        return None
    return number


def is_url(path):
    return str(path).lower().startswith(('http://', 'https://'))


# Query parameters whose values are never written to the log.
_SECRET_PARAMS = re.compile(r'(key|token|secret|password|passwd|appid|app_id|client_id|'
                            r'auth|signature|sig)$', re.I)


def redact_url(url):
    """https://x/y?appid=abc123&q=1 -> https://x/y?appid=***&q=1"""
    try:
        from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
        parts = urlsplit(url)
        query = [(k, '***' if _SECRET_PARAMS.search(k) else v)
                 for k, v in parse_qsl(parts.query, keep_blank_values=True)]
        netloc = parts.netloc
        if '@' in netloc:                                   # user:pass@host
            netloc = '***@' + netloc.split('@', 1)[1]
        return urlunsplit((parts.scheme, netloc, parts.path, urlencode(query, safe='*'),
                           ''))
    except Exception:
        return '(url)'


def station_placeholders(config_dict):
    """{latitude}, {longitude}, {altitude} (metres) from [Station], for URLs."""
    stn = config_dict.get('Station', {}) or {}
    out = {}
    for key in ('latitude', 'longitude'):
        try:
            out[key] = float(stn[key])
        except (KeyError, TypeError, ValueError):
            pass
    try:
        alt = stn['altitude']
        if isinstance(alt, str):
            alt = [a.strip() for a in alt.split(',')]
        out['altitude'] = round(weewx.units.convert(
            weewx.units.ValueTuple(float(alt[0]), alt[1], 'group_altitude'), 'meter')[0], 1)
    except (KeyError, TypeError, ValueError, IndexError):
        pass
    return out


def resolve_url(url, placeholders):
    """Fill {latitude}/{longitude}/{altitude}. Unknown {names} are an error."""
    try:
        return url.format(**placeholders)
    except (KeyError, IndexError, ValueError) as e:
        raise ValueError("cannot fill %s in the URL (known: %s)"
                         % (e, ', '.join('{%s}' % k for k in sorted(placeholders)) or 'none'))


_PLACEHOLDER_RE = re.compile(r'\{(latitude|longitude|altitude)\}')
_SECRET_NAMES = re.compile(r'(key|token|secret|password|passwd|auth|cookie|session|'
                           r'appid|app_id|client_id|signature|sig)', re.I)


def fill_placeholders(text, placeholders):
    """{latitude}/{longitude}/{altitude} in params, form and body values.
    Other braces (JSON bodies, secrets) are left alone."""
    return _PLACEHOLDER_RE.sub(lambda m: str(placeholders.get(m.group(1), m.group(0))),
                               str(text))


class RequestSpec(object):
    """Everything sent to the server, resolved: URL (with params), method,
    body, headers and auth. Two sources with equal specs share a fetcher."""

    def __init__(self, source, placeholders):
        from urllib.parse import urlencode
        self.url = resolve_url(source.path, placeholders)
        params = {k: fill_placeholders(v, placeholders) for k, v in source.params.items()}
        if params:
            self.url += ('&' if '?' in self.url else '?') + urlencode(params)
        self.headers = dict(source.headers)
        self.data = None
        self.content_type = source.content_type
        if source.form:
            self.data = urlencode({k: fill_placeholders(v, placeholders)
                                   for k, v in source.form.items()}).encode('utf-8')
            self.content_type = self.content_type or 'application/x-www-form-urlencoded'
        elif source.body is not None:
            body = fill_placeholders(source.body, placeholders)
            self.data = body.encode('utf-8')
            self.content_type = self.content_type or (
                'application/json' if body.lstrip()[:1] in ('{', '[') else 'text/plain')
        self.method = source.method or ('POST' if self.data is not None else 'GET')
        self.auth = source.auth

        # Strings that must never reach a log.
        secrets = set()
        for mapping in (source.params, source.form, source.headers):
            secrets.update(v for k, v in mapping.items() if _SECRET_NAMES.search(k))
        if source.auth:
            for k, v in source.auth.items():
                if isinstance(v, dict):
                    secrets.update(v.values())
                elif k in ('username', 'password', 'token', 'client_id', 'client_secret',
                           'body'):
                    secrets.add(v)
        self.secrets = {str(x) for x in secrets if x and len(str(x)) >= 4}

    def key(self):
        return repr((self.url, self.method, sorted(self.headers.items()), self.data,
                     self.content_type, sorted((self.auth or {}).items(), key=str)))


def scrub(text, secrets):
    """Replace every known secret in a message with ***."""
    text = str(text)
    for secret in sorted(secrets, key=len, reverse=True):
        text = text.replace(secret, '***')
    return text


class RateLimited(Exception):
    """A fetch was refused locally to stay within the API's limits."""

    def __init__(self, msg, wait):
        super(RateLimited, self).__init__(msg)
        self.wait = wait


def parse_retry_after(value, now=None):
    """Retry-After header (seconds, or an HTTP date) -> seconds, or None."""
    if not value:
        return None
    now = time.time() if now is None else now
    value = str(value).strip()
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        import email.utils
        return max(0.0, email.utils.parsedate_to_datetime(value).timestamp() - now)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None


def cache_dir_for(config_dict):
    """Where URL responses and request logs are kept between restarts:
    [DataIngest] cache_dir, else <SQLITE_ROOT>/dataingest_cache (the same
    place as the databases, which the WeeWX user can always write to).
    None if neither can be worked out (no persistence)."""
    root = config_dict.get('WEEWX_ROOT')
    custom = service_section(config_dict).get('cache_dir')
    if custom:
        return os.path.join(root or '', os.path.expanduser(str(custom)))
    try:
        sqlite_root = config_dict['DatabaseTypes']['SQLite']['SQLITE_ROOT']
    except (KeyError, TypeError):
        sqlite_root = 'archive' if root else None
    if sqlite_root is None:
        return None
    return os.path.join(root or '', sqlite_root, 'dataingest_cache')


class _FileLock(object):
    """Exclusive lock shared by weewxd and the command-line helper, so the
    two never fetch the same URL in the same moment. No-op where fcntl is
    unavailable."""

    def __init__(self, path):
        self.path = path
        self.fd = None

    def __enter__(self):
        if self.path is None:
            return self
        try:
            import fcntl
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            self.fd = open(self.path, 'a')
            fcntl.flock(self.fd, fcntl.LOCK_EX)
        except (ImportError, OSError):
            self.fd = None
        return self

    def __exit__(self, *exc):
        if self.fd is not None:
            import fcntl
            fcntl.flock(self.fd, fcntl.LOCK_UN)
            self.fd.close()
            self.fd = None


class UrlFetcher(object):
    """Fetches one URL in a background thread and keeps the latest good
    response. WeeWX's main loop only reads that copy, so a slow or dead API
    can never hold it up.

    Request limits are enforced here and survive restarts (state is kept in
    cache_dir):

    * never more often than poll_interval after a success;
    * after a failure, wait retry_interval (default poll_interval), doubling
      for each consecutive failure up to max(poll_interval, 1 hour);
    * a 429/503 Retry-After from the server is always honoured;
    * at most max_requests_per_day in any rolling 24 hours (if set);
    * conditional requests (ETag / Last-Modified), so unchanged data costs
      the server almost nothing.

    Several sources with the same URL and headers share one fetcher.
    """

    def __init__(self, source, spec, cache_dir=None):
        import threading
        if isinstance(spec, str):       # a plain URL
            spec = RequestSpec(SourceConfig(source.name, spec, {}), {})
            spec.headers = dict(source.headers)
        self.spec = spec
        self.source = source            # the first source using this URL
        self.sources = [source]
        self.url = spec.url
        self.poll_interval = source.poll_interval
        self.retry_interval = source.retry_interval or source.poll_interval
        self.timeout = source.timeout
        self.max_per_day = source.max_requests_per_day
        self.headers = dict(spec.headers)
        self.lock = threading.Lock()
        self.text = None
        self.fetched_at = None          # last successful fetch (or 304)
        self.last_error = None
        self._failing = False
        self._stop = threading.Event()
        self._thread = None
        self._limit_logged = False
        self.state = {'requests': [], 'failures': 0, 'last_attempt': None,
                      'last_success': None, 'not_before': None,
                      'etag': None, 'last_modified': None,
                      'token': None, 'token_expires': None, 'token_requests': []}
        self.cache_path = self.state_path = self.lock_path = None
        if cache_dir:
            import hashlib
            key = hashlib.sha1(spec.key().encode('utf-8')).hexdigest()[:12]
            base = os.path.join(cache_dir, '%s-%s' % (re.sub(r'[^A-Za-z0-9_-]', '_',
                                                            source.name), key))
            self.cache_path = base + '.cache'
            self.state_path = base + '.state.json'
            self.lock_path = base + '.lock'
            self.cache_dir = cache_dir
        self._load()

    # -- sharing -------------------------------------------------------------

    def add_source(self, source):
        """Another source with the same URL: obey the strictest settings."""
        self.sources.append(source)
        self.poll_interval = max(self.poll_interval, source.poll_interval)
        self.retry_interval = max(self.retry_interval,
                                  source.retry_interval or source.poll_interval)
        self.timeout = max(self.timeout, source.timeout)
        limits = [x for x in (self.max_per_day, source.max_requests_per_day) if x]
        self.max_per_day = min(limits) if limits else 0

    # -- persistence -----------------------------------------------------------

    def _load(self):
        """Restore the request log and the last good response."""
        if not self.state_path:
            return
        try:
            with open(self.state_path, 'r', encoding='utf-8') as f:
                saved = json.load(f)
            if isinstance(saved, dict):
                self.state.update({k: saved[k] for k in self.state if k in saved})
        except (OSError, ValueError):
            pass
        if self.text is None and self.state.get('last_success'):
            try:
                with open(self.cache_path, 'r', encoding='utf-8') as f:
                    self.text = f.read()
                self.fetched_at = self.state['last_success']
            except OSError:
                pass

    def _save_state(self):
        if not self.state_path:
            return
        try:
            os.makedirs(self.cache_dir, exist_ok=True)
            tmp = self.state_path + '.tmp'
            # Owner-only: it may hold an access token.
            fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                json.dump(self.state, f)
            os.replace(tmp, self.state_path)
        except OSError as e:
            log.warning("Source '%s': could not save request log in %s: %s",
                        self.source.name, self.cache_dir, e)

    def _save_cache(self, text):
        for path in filter(None, (self.cache_path, self.source.save_to)):
            tmp = path + '.tmp'
            try:
                d = os.path.dirname(path)
                if d:
                    os.makedirs(d, exist_ok=True)
                with open(tmp, 'w', encoding='utf-8') as f:
                    f.write(text)
                os.replace(tmp, path)
            except OSError as e:
                log.warning("Source '%s': could not save to %s: %s", self.source.name, path, e)

    # -- scheduling ----------------------------------------------------------------

    def _recent_requests(self, now):
        return [t for t in self.state.get('requests', []) if now - t < 86400]

    def _interval(self):
        """Wait after the last attempt: poll_interval after a success,
        retry_interval doubling per consecutive failure after a failure."""
        n = self.state.get('failures', 0)
        if not n:
            return self.poll_interval
        cap = max(self.poll_interval, 3600)
        return min(self.retry_interval * (2 ** (n - 1)), cap)

    def wait_time(self, now=None, force=False):
        """(seconds until a request is allowed, reason). force skips the
        poll/retry interval but never the daily cap or a server Retry-After."""
        now = time.time() if now is None else now
        waits = []
        last = self.state.get('last_attempt')
        if last and not force:
            waits.append((last + self._interval() - now, 'poll interval'))
        if self.state.get('not_before'):
            waits.append((self.state['not_before'] - now, 'server asked to wait (Retry-After)'))
        if self.max_per_day:
            recent = self._recent_requests(now)
            if len(recent) >= self.max_per_day:
                waits.append((min(recent) + 86400 - now,
                              'max_requests_per_day = %d reached' % self.max_per_day))
        if not waits:
            return 0.0, None
        wait, why = max(waits)
        return max(0.0, wait), why if wait > 0 else None

    # -- fetching --------------------------------------------------------------------

    def fetch_once(self, force=True):
        """Fetch now if the limits allow (force: ignore only the poll/retry
        interval). Returns the response text; raises RateLimited when a
        request would break a limit, or another exception on failure."""
        with _FileLock(self.lock_path):
            self._load()                       # the CLI may have fetched meanwhile
            now = time.time()
            wait, why = self.wait_time(now, force=force)
            if wait > 0:
                raise RateLimited("next request allowed in %.0f s (%s)" % (wait, why), wait)
            self.state['requests'] = self._recent_requests(now) + [now]
            self.state['last_attempt'] = now
            try:
                text = self._request()
            except Exception as e:
                self.state['failures'] = self.state.get('failures', 0) + 1
                retry_after = getattr(e, 'retry_after', None)
                if retry_after:
                    self.state['not_before'] = now + retry_after
                self._save_state()
                raise
            self.state['failures'] = 0
            self.state['not_before'] = None
            self.state['last_success'] = now
            if text is not None:
                self._save_cache(text)
            self._save_state()
        with self.lock:
            if text is not None:
                self.text = text
            self.fetched_at, self.last_error = now, None
        return self.text

    def _request(self):
        """One HTTP request. Returns the text, None for 304 Not Modified."""
        import gzip
        import urllib.error
        import urllib.request
        headers = {'User-Agent': 'weewx-data-ingest/%s (WeeWX %s)' % (VERSION,
                                                                     weewx.__version__),
                   'Accept': '*/*', 'Accept-Encoding': 'gzip'}
        if self.text is not None:
            if self.state.get('etag'):
                headers['If-None-Match'] = self.state['etag']
            if self.state.get('last_modified'):
                headers['If-Modified-Since'] = self.state['last_modified']
        headers.update(self.headers)
        url = self.url
        if self.spec.data is not None:
            headers['Content-Type'] = self.spec.content_type
        auth_headers, auth_params = self._auth()
        headers.update(auth_headers)
        if auth_params:
            from urllib.parse import urlencode
            url += ('&' if '?' in url else '?') + urlencode(auth_params)
        req = urllib.request.Request(url, data=self.spec.data, headers=headers,
                                     method=self.spec.method)
        try:
            resp = urllib.request.urlopen(req, timeout=self.timeout)
        except urllib.error.HTTPError as e:
            if e.code == 304 and self.text is not None:
                return None
            if e.code in (401, 403) and self.state.get('token'):
                # Token revoked or expired early: get a new one next time.
                self.state['token'] = self.state['token_expires'] = None
            retry_after = None
            if e.code in (429, 503):
                retry_after = parse_retry_after(e.headers.get('Retry-After'))
            err = ValueError("HTTP %d %s%s" % (e.code, e.reason, " (Retry-After %.0f s)"
                                               % retry_after if retry_after else ""))
            err.retry_after = retry_after
            raise err
        with resp:
            body = resp.read(MAX_RESPONSE_BYTES + 1)
            charset = resp.headers.get_content_charset() or 'utf-8'
            encoding = (resp.headers.get('Content-Encoding') or '').lower()
            etag, modified = resp.headers.get('ETag'), resp.headers.get('Last-Modified')
        if len(body) > MAX_RESPONSE_BYTES:
            raise ValueError("response larger than %d bytes" % MAX_RESPONSE_BYTES)
        if encoding == 'gzip':
            body = gzip.decompress(body)
            if len(body) > MAX_RESPONSE_BYTES:
                raise ValueError("response larger than %d bytes" % MAX_RESPONSE_BYTES)
        text = body.decode('utf-8-sig' if charset.lower() in ('utf-8', 'utf8') else charset,
                           errors='replace')
        if not text.strip():
            raise ValueError("empty response")
        # Never replace good data with an error page or a truncated reply.
        parse_text(text, self.source.format)
        self.state['etag'], self.state['last_modified'] = etag, modified
        return text

    # -- authentication --------------------------------------------------------------

    @staticmethod
    def _basic(user, password):
        import base64
        return 'Basic ' + base64.b64encode(('%s:%s' % (user, password))
                                           .encode('utf-8')).decode('ascii')

    def _auth(self):
        """(headers, query params) that authenticate the data request."""
        a = self.spec.auth
        if not a:
            return {}, {}
        if a['type'] == 'basic':
            return {'Authorization': self._basic(a['username'], a['password'])}, {}
        token = a['token'] if a['type'] == 'bearer' else self._token()
        if a.get('param'):
            return {}, {a['param']: token}
        return {a['header']: ('%s %s' % (a['prefix'], token)) if a['prefix'] else token}, {}

    def _token(self):
        """A valid access token: the saved one, or a new one from token_url.
        Requested only when missing or within a minute of expiring."""
        import urllib.error
        import urllib.request
        from urllib.parse import urlencode
        a = self.spec.auth
        now = time.time()
        if self.state.get('token') and (self.state.get('token_expires') or 0) > now + 60:
            return self.state['token']
        url = a['token_url']
        if a['params']:
            url += ('&' if '?' in url else '?') + urlencode(a['params'])
        headers = {'User-Agent': 'weewx-data-ingest/%s' % VERSION, 'Accept': 'application/json'}
        headers.update(a['headers'])
        form = dict(a['form'])
        if a['type'] == 'oauth2':
            form.setdefault('grant_type', 'client_credentials')
            if a.get('scope'):
                form.setdefault('scope', a['scope'])
            if a['client_auth'] == 'basic':
                headers['Authorization'] = self._basic(a['client_id'], a['client_secret'])
            else:
                form.setdefault('client_id', a['client_id'])
                form.setdefault('client_secret', a['client_secret'])
        if a.get('body') and not form:
            data = a['body'].encode('utf-8')
            headers['Content-Type'] = a.get('content_type') or 'application/json'
        else:
            data = urlencode(form).encode('utf-8') if form else None
            if data is not None:
                headers['Content-Type'] = 'application/x-www-form-urlencoded'
        method = str(a.get('method', 'POST' if data is not None else 'GET')).upper()
        self.state['token_requests'] = [t for t in self.state.get('token_requests', [])
                                        if now - t < 86400] + [now]
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    url, data=data, headers=headers, method=method),
                    timeout=self.timeout) as resp:
                body = resp.read(MAX_RESPONSE_BYTES).decode('utf-8', errors='replace')
        except urllib.error.HTTPError as e:
            raise ValueError("token request: HTTP %d %s" % (e.code, e.reason))
        try:
            reply = json.loads(body)
        except ValueError:
            raise ValueError("token request: response is not JSON")
        found, token = lookup(reply, a['token_path'])
        if not found or not token or isinstance(token, (dict, list)):
            raise ValueError("token request: no '%s' in the response" % a['token_path'])
        found, expires = lookup(reply, a['expires_path'])
        lifetime = to_number(expires) if found else None
        self.state['token'] = str(token)
        self.state['token_expires'] = now + (lifetime if lifetime else a['token_lifetime'])
        self.spec.secrets.add(str(token))
        log.debug("Source '%s': new access token, valid %.0f s", self.source.name,
                  self.state['token_expires'] - now)
        return self.state['token']

    def latest(self):
        with self.lock:
            return self.text, self.fetched_at

    def status(self, now=None):
        """One line for logs and the command line."""
        now = time.time() if now is None else now
        wait, why = self.wait_time(now)
        parts = ["every %d s" % self.poll_interval]
        if self.max_per_day:
            parts.append("max %d/day" % self.max_per_day)
        parts.append("%d request(s) in the last 24 h" % len(self._recent_requests(now)))
        if self.spec.auth and self.spec.auth['type'] in ('oauth2', 'token'):
            tokens = [t for t in self.state.get('token_requests', []) if now - t < 86400]
            parts.append("%d token request(s)" % len(tokens))
        if self.fetched_at:
            parts.append("data %.0f s old" % (now - self.fetched_at))
        parts.append("next request in %.0f s (%s)" % (wait, why) if wait else
                     "next request now")
        return ', '.join(parts)

    # -- background thread ------------------------------------------------------------

    def _run(self):
        src = self.source
        while not self._stop.is_set():
            wait, why = self.wait_time()
            if wait > 0:
                # Re-check at least every 5 minutes: the CLI may have fetched.
                self._stop.wait(min(wait, 300))
                continue
            try:
                self.fetch_once(force=False)
                if self._failing:
                    log.info("Source '%s': fetching %s again", src.name, redact_url(self.url))
                    self._failing = False
                self._limit_logged = False
                log.debug("Source '%s': fetched %s", src.name, redact_url(self.url))
            except RateLimited as e:
                if not self._limit_logged:
                    log.info("Source '%s': %s", src.name, e)
                    self._limit_logged = True
            except Exception as e:
                msg = scrub(e, self.spec.secrets)
                if msg != self.last_error:
                    log.warning("Source '%s': fetch of %s failed: %s -- keeping the last "
                                "good response; next try in %.0f s", src.name,
                                redact_url(self.url), msg, self.wait_time()[0])
                self.last_error, self._failing = msg, True

    def start(self):
        import threading
        self._thread = threading.Thread(target=self._run, name='dataingest-%s' % self.source.name,
                                        daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=self.timeout + 1)


def make_reader(source, placeholders, cache_dir=None, fetchers=None):
    """SourceReader for a source; URL sources get an (unstarted) fetcher,
    shared with any other source using the same URL and headers when a
    fetchers dict is passed. Raises ValueError if the URL cannot be resolved."""
    if not source.is_url:
        return SourceReader(source)
    spec = RequestSpec(source, placeholders)
    key = spec.key()
    if fetchers is not None and key in fetchers:
        fetcher = fetchers[key]
        fetcher.add_source(source)
    else:
        fetcher = UrlFetcher(source, spec, cache_dir)
        if fetchers is not None:
            fetchers[key] = fetcher
    return SourceReader(source, fetcher)


class SourceReader(object):
    """Reads one source -- a local file, or a URL via UrlFetcher --
    re-parsing only when it changes, and keeping the last good parse if a
    read catches a file half-written or a response is garbled."""

    def __init__(self, source, fetcher=None):
        self.source = source
        self.fetcher = fetcher
        self.mtime = None          # file mtime, or time of the fetch parsed
        self.bad_stamp = None      # version that failed to parse
        self.data = None
        self.errors_logged = set()

    def _note(self, key, level, msg, *args):
        # Log each distinct problem once, until the source recovers.
        if key not in self.errors_logged:
            log.log(level, msg, *args)
            self.errors_logged.add(key)

    def _stamp(self):
        """(stamp, where) of what is available now, or (None, None).
        The stamp is the file's mtime, or the time of the last good fetch."""
        src = self.source
        if self.fetcher is not None:
            _text, stamp = self.fetcher.latest()
            if stamp is None:
                self._note('nodata', logging.INFO,
                           "Source '%s': no successful fetch yet -- skipping until there is",
                           src.name)
                return None, None
            self.errors_logged.discard('nodata')
            return stamp, redact_url(self.fetcher.url)
        try:
            stamp = os.path.getmtime(src.path)
        except OSError:
            self._note('missing', logging.WARNING,
                       "Source '%s': %s not found -- skipping until it appears",
                       src.name, src.path)
            self.mtime = self.data = None
            return None, None
        self.errors_logged.discard('missing')
        return stamp, src.path

    def _text(self):
        if self.fetcher is not None:
            return self.fetcher.latest()[0]
        with open(self.source.path, 'r', encoding='utf-8-sig') as f:
            return f.read()

    def read(self, now=None, force=False):
        """Return (data, age_seconds) or (None, None). force re-reads a
        file even if its mtime is unchanged (a fetched response is already
        the latest there is)."""
        src = self.source
        now = time.time() if now is None else now
        stamp, where = self._stamp()
        if stamp is None:
            return None, None

        changed = stamp != self.mtime or self.data is None or \
            (force and self.fetcher is None)
        if changed and (stamp != self.bad_stamp or (force and self.fetcher is None)):
            try:
                parsed = parse_text(self._text(), src.format)
                ok, node = lookup(parsed, src.json_path)
                if not ok:
                    self._note('json_path', logging.WARNING,
                               "Source '%s': path '%s' not found in %s",
                               src.name, src.json_path, where)
                    self.bad_stamp = stamp
                    if self.data is None:
                        return None, None
                else:
                    self.errors_logged.discard('json_path')
                    self.errors_logged.discard('parse')
                    self.data, self.mtime, self.bad_stamp = node, stamp, None
            except (OSError, ValueError, UnicodeDecodeError) as e:
                # Usually a writer mid-rewrite, or a garbled response. Keep
                # the last good copy; don't re-parse this same version.
                self._note('parse', logging.WARNING,
                           "Source '%s': could not parse %s (%s)%s", src.name, where, e,
                           " -- using previous data" if self.data is not None else "")
                self.bad_stamp = stamp
                if self.data is None:
                    return None, None
        elif self.data is None:
            return None, None

        age = now - self.mtime
        if src.max_age and age > src.max_age:
            self._note('stale', logging.INFO,
                       "Source '%s': data from %s is %.0f s old (max_age %d) -- not injecting "
                       "until refreshed", src.name, where, age, src.max_age)
            return None, age
        self.errors_logged.discard('stale')
        return self.data, age

    def values(self, now=None, force=False):
        """{db column: float} for every mapped key present and numeric."""
        data, _age = self.read(now, force)
        if data is None:
            return {}
        out = {}
        for key, column in self.source.mapping.items():
            found, raw = lookup(data, key)
            if not found:
                continue
            number = to_number(raw, self.source.text_values)
            if number is None:
                log.debug("Source '%s': %s=%r is not numeric, skipped",
                          self.source.name, key, raw)
                continue
            out[column] = number
        return out


def configured_db_units(config_dict):
    """The unit system WeeWX writes to the database, from weewx.conf.

    WeeWX converts every loop packet and archive record to
    [StdConvert] target_unit (default US) before it is archived -- provided
    StdConvert is in process_services. Returns (unit system or None, reason).
    """
    services = (config_dict.get('Engine', {}) or {}).get('Services', {}) or {}
    if 'process_services' in services:
        procs = services['process_services']
        if isinstance(procs, str):
            procs = [p.strip() for p in procs.split(',')]
        if not any(str(p).endswith('StdConvert') for p in procs):
            return None, "StdConvert is not in process_services: the database " \
                         "uses the driver's units"
    nick = (config_dict.get('StdConvert', {}) or {}).get('target_unit', 'US')
    try:
        return weewx.units.unit_constants[str(nick).upper()], \
            "[StdConvert] target_unit = %s" % nick
    except KeyError:
        return None, "[StdConvert] target_unit = %s is not a unit system" % nick


def unit_system_name(system):
    return weewx.units.unit_nicknames.get(system, 'unknown') if system is not None \
        else 'unknown'


def convert_values(values, from_system, to_system, units=None):
    """Convert {column: value} read from a file into to_system (the units
    of the packet or record it is going into).

    units      per-column unit names for the file ({'o3x': 'degree_C'})
    from_system the file's unit system for every other column

    Columns with no unit group cannot be converted and are passed through.
    A value whose conversion fails is dropped rather than stored wrong."""
    units = units or {}
    if to_system is None:
        return dict(values)
    out = {}
    for column, value in values.items():
        group = weewx.units.obs_group_dict.get(column)
        try:
            if column in units and group:
                target = weewx.units.getStandardUnitType(to_system, column)[0]
                out[column] = weewx.units.convert(
                    weewx.units.ValueTuple(value, units[column], group), target).value
            elif group and from_system is not None and from_system != to_system:
                vt = weewx.units.as_value_tuple({column: value, 'usUnits': from_system},
                                                column)
                out[column] = weewx.units.convertStd(vt, to_system).value
            else:
                out[column] = value
        except (KeyError, ValueError, TypeError) as e:
            log.debug("Could not convert %s=%r: %s -- dropped", column, value, e)
    return out


def unconvertible_columns(source, db_units):
    """Columns whose file units are declared as different from the
    database's but which have no unit group, so they cannot be converted.
    Their values are refused: storing them as read would put a second unit
    into a database column. (A column with no group and no declared units
    is taken to be in the database's units already, and stored as read.)"""
    out = set()
    for col in source.columns:
        if weewx.units.obs_group_dict.get(col) is not None:
            continue
        if col in source.units or (source.unit_system is not None and
                                   source.unit_system != db_units):
            out.add(col)
    return out


def conversion_problems(source, db_units):
    """Human-readable problems with a source's unit settings."""
    problems = []
    if source.unit_system is None and not source.units:
        return problems
    for col in sorted(unconvertible_columns(source, db_units)):
        problems.append("%s has no unit group, so it cannot be converted to the database's "
                        "units -- its values will NOT be stored (a column must never mix "
                        "units). Add [%s][[groups]] %s = group_..." % (col, SECTION, col))
    for col in source.columns:
        group = weewx.units.obs_group_dict.get(col)
        if group is None:
            continue
        if col in source.units and db_units is not None:
            target = weewx.units.getStandardUnitType(db_units, col)[0]
            try:
                weewx.units.convert(weewx.units.ValueTuple(1.0, source.units[col], group),
                                    target)
            except KeyError:
                problems.append("%s: cannot convert %s to %s (%s) -- values will be "
                                "dropped" % (col, source.units[col], target, group))
    return problems


# ---------------------------------------------------------------------------
# Day / night
# ---------------------------------------------------------------------------

def is_daytime(time_ts, lat, lon, altitude_m=None):
    """True between sunrise and sunset, None if it can't be worked out."""
    try:
        import weewx.almanac
        alm = weewx.almanac.Almanac(time_ts, lat, lon, altitude=altitude_m)
        try:
            # Precise when pyephem/skyfield are installed.
            alt = alm.sun.altitude.raw
            if alt is not None:
                return alt > 0
        except Exception:
            pass
        rise, sset = alm.sunrise.raw, alm.sunset.raw
        if rise is None or sset is None:
            return None
        return rise <= time_ts < sset
    except Exception as e:
        log.debug("Day/night calculation failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Database columns: where does each mapped column get archived?
# ---------------------------------------------------------------------------

def wanted_columns(sources, default_policy):
    """{column: policy} in first-seen order. If two sources map to the same
    column with different policies, 'add' beats 'separate' beats 'warn'."""
    rank = {'warn': 0, 'separate': 1, 'add': 2}
    wanted = {}
    for s in sources:
        if not s.enable:
            continue
        policy = s.missing_columns or default_policy
        for col in s.columns:
            if col not in wanted or rank[policy] > rank[wanted[col]]:
                wanted[col] = policy
    return wanted


def is_numeric_sql(sql_type):
    sql_type = str(sql_type or '').upper()
    return not sql_type.startswith(NON_NUMERIC_SQL)


def column_types(manager):
    """{column: SQL type} for a manager's main table."""
    return {row[1]: row[2] for row in manager.connection.genSchemaOf(manager.table_name)}


class ColumnPlan(object):
    """Decides, for each mapped column, where it will be archived.

    main        already in the main archive and numeric
    add_main    to be added to the main archive        (policy 'add')
    separate    already in the separate database        (policy 'separate')
    add_sep     to be added to the separate database    (policy 'separate')
    unarchived  will not be archived                     (policy 'warn')
    unsuitable  in the main archive but not numeric (then treated as missing)
    """

    def __init__(self, wanted, main_types, sep_columns=()):
        self.main, self.add_main, self.separate, self.add_sep = [], [], [], []
        self.unarchived, self.unsuitable = [], []
        sep_columns = set(sep_columns or ())
        for col, policy in wanted.items():
            if col in main_types:
                if is_numeric_sql(main_types[col]):
                    self.main.append(col)
                    continue
                self.unsuitable.append(col)
                if policy == 'add':
                    # Can't add a column that is already there.
                    self.unarchived.append(col)
                    continue
            if policy == 'separate':
                (self.separate if col in sep_columns else self.add_sep).append(col)
            elif policy == 'add':
                self.add_main.append(col)
            else:
                self.unarchived.append(col)

    @property
    def separate_columns(self):
        return self.separate + self.add_sep

    @property
    def needs_separate(self):
        return bool(self.separate_columns)

    def describe(self):
        lines = []
        for label, cols in (("main archive", self.main),
                            ("main archive (to be added)", self.add_main),
                            ("separate database", self.separate),
                            ("separate database (to be added)", self.add_sep),
                            ("NOT archived", self.unarchived),
                            ("exists in main archive but is not numeric", self.unsuitable)):
            if cols:
                lines.append("%s: %s" % (label, ', '.join(cols)))
        return lines


def ensure_separate_binding(config_dict, binding=SEPARATE_BINDING):
    """Give config_dict a binding (and database) for the separate database
    if weewx.conf does not define one. In memory only."""
    bindings = config_dict.setdefault('DataBindings', {})
    if binding not in bindings:
        bindings[binding] = dict(DEFAULT_SEPARATE_BINDING)
        log.info("Using built-in defaults for data binding '%s' (dataingest.sdb)", binding)
    database = bindings[binding].get('database', SEPARATE_DATABASE)
    databases = config_dict.setdefault('Databases', {})
    if database not in databases:
        databases[database] = dict(DEFAULT_SEPARATE_DATABASE)
    return binding


def refresh_manager(manager):
    """Re-read the column list after add_column, which does not update
    the manager's own cache."""
    import weewx.manager
    manager.sqlkeys = manager.connection.columnsOf(manager.table_name)
    if isinstance(manager, weewx.manager.DaySummaryManager):
        weewx.manager.DaySummaryManager._create_sync(manager)


def add_columns(manager, columns):
    added = []
    for col in columns:
        if col not in manager.sqlkeys:
            manager.add_column(col, 'REAL')
            added.append(col)
    if added:
        refresh_manager(manager)
    return added


def build_plan(get_manager, sources, default_policy, main_binding, sep_binding):
    """Work out a ColumnPlan. get_manager(binding) returns an open manager
    (creating the database if needed)."""
    wanted = wanted_columns(sources, default_policy)
    main_types = column_types(get_manager(main_binding))
    plan = ColumnPlan(wanted, main_types)
    if plan.needs_separate:
        sep_cols = get_manager(sep_binding).sqlkeys
        plan = ColumnPlan(wanted, main_types, sep_cols)
    return plan


def apply_plan(get_manager, plan, main_binding, sep_binding):
    """Create what the plan needs. Returns (added_main, added_sep)."""
    added_main = add_columns(get_manager(main_binding), plan.add_main) if plan.add_main else []
    added_sep = add_columns(get_manager(sep_binding), plan.add_sep) if plan.add_sep else []
    if added_main:
        plan.main += added_main
        plan.add_main = [c for c in plan.add_main if c not in added_main]
    if added_sep:
        plan.separate += added_sep
        plan.add_sep = [c for c in plan.add_sep if c not in added_sep]
    return added_main, added_sep


def register_groups(section):
    """[DataIngest][[groups]] column = group_xxx -> weewx.units.obs_group_dict,
    so values are unit-converted and labelled in reports."""
    groups = section.get('groups', {}) or {}
    for col, group in groups.items():
        if isinstance(group, dict):
            continue
        group = str(group).strip()
        if not group.startswith('group_'):
            log.error("[[groups]] %s = %s: unit group names start with 'group_'", col, group)
            continue
        current = weewx.units.obs_group_dict.get(col)
        if current is not None and current != group:
            # Re-grouping an existing column would convert new values
            # differently from what is already stored in it.
            log.error("[[groups]] %s = %s ignored: %s is already %s in WeeWX, and changing "
                      "it would mix units in the database", col, group, col, current)
            continue
        weewx.units.obs_group_dict[col] = group
    return groups


def register_extractors(config_dict, sources):
    """Per-source 'extractor' -> [Accumulator] (unless the user already set
    one), so StdArchive picks it up when it initialises the accumulators."""
    acc = config_dict.setdefault('Accumulator', {})
    for s in sources:
        if not (s.enable and s.extractor):
            continue
        for col in s.columns:
            if col in acc:
                continue
            acc[col] = {'extractor': s.extractor}


# ---------------------------------------------------------------------------
# The service
# ---------------------------------------------------------------------------

class DataIngestService(StdService):
    """Inject external file data into loop packets and archive records.

    Register in [Engine][[Services]] data_services. If another data
    service uses or publishes loop packets (an MQTT or live-data
    service, say), list this one before it so it sees the injected values.
    """

    def __init__(self, engine, config_dict):
        super(DataIngestService, self).__init__(engine, config_dict)
        section = service_section(config_dict)
        log.info("DataIngestService version %s", VERSION)
        self.readers = []
        self.plan = None
        self.sep_columns = []

        if not to_bool(section.get('enable', True)):
            log.info("DataIngestService disabled ([%s] enable = false)", SECTION)
            return

        sources = load_sources(config_dict)
        placeholders = station_placeholders(config_dict)
        self.cache_dir = cache_dir_for(config_dict)
        self.fetchers = {}              # (url, headers) -> UrlFetcher, shared
        for s in sources:
            if not s.enable:
                log.info("Source '%s': disabled", s.name)
                continue
            if not s.mapping:
                log.warning("Source '%s': no mapping configured -- ignored", s.name)
                continue
            try:
                reader = make_reader(s, placeholders, self.cache_dir, self.fetchers)
            except ValueError as e:
                log.error("Source '%s': %s -- disabled", s.name, e)
                continue
            self.readers.append(reader)
            where = s.path if reader.fetcher is None else redact_url(reader.fetcher.url)
            if reader.fetcher is not None and reader.fetcher.source is not s:
                where += " (shared with '%s': one request serves both)" \
                    % reader.fetcher.source.name
            log.info("Source '%s': %s -> %s (loop=%s archive=%s when=%s max_age=%s)",
                     s.name, where, ', '.join(s.columns), s.inject_loop, s.inject_archive,
                     s.when, s.max_age or 'none')
        if not self.readers:
            log.warning("DataIngestService: no active sources configured")
            return

        register_groups(section)
        # Must happen now: StdArchive (constructed after us) reads
        # [Accumulator] in its __init__.
        register_extractors(config_dict, [r.source for r in self.readers])

        try:
            stn = engine.stn_info
            self.lat, self.lon = stn.latitude_f, stn.longitude_f
            self.altitude_m = weewx.units.convert(stn.altitude_vt, 'meter')[0]
        except Exception:
            self.lat = self.lon = self.altitude_m = None
        self._day_cache = (None, None)  # (minute, is_day)

        self.main_binding = section.get('data_binding', MAIN_BINDING)
        self.sep_binding = section.get('separate_binding', SEPARATE_BINDING)
        self.default_policy = _choice(section.get('missing_columns'), MISSING_CHOICES,
                                      'separate', 'missing_columns', SECTION)

        # The unit system archived data ends up in. Files without a
        # unit_system are taken to be in it. Confirmed against the database
        # itself at STARTUP.
        self.db_units, self.db_units_reason = configured_db_units(config_dict)

        # Columns are checked at STARTUP, not here: StdArchive (an archive
        # service, constructed after this data service) creates the main
        # database on a first run.
        # Start fetching now, so URL data is usually ready for the first
        # loop packets. A response saved before a restart is used straight
        # away; the next request waits until the limits allow it.
        for fetcher in self.fetchers.values():
            log.info("Source '%s': %s", fetcher.source.name, fetcher.status())
            if fetcher.cache_path is None:
                log.warning("Source '%s': no cache directory, so request limits are not "
                            "kept across restarts. Set [%s] cache_dir.",
                            fetcher.source.name, SECTION)
            fetcher.start()

        self.bind(weewx.STARTUP, self.startup)
        if any(r.source.inject_loop for r in self.readers):
            self.bind(weewx.NEW_LOOP_PACKET, self.new_loop_packet)
        if any(r.source.inject_archive for r in self.readers):
            self.bind(weewx.NEW_ARCHIVE_RECORD, self.new_archive_record)

    # -- database columns ------------------------------------------------

    def _get_manager(self, binding):
        return self.engine.db_binder.get_manager(binding, initialize=True)

    def _check_units(self, sources):
        """Confirm the database unit system and report unit problems."""
        try:
            actual = self._get_manager(self.main_binding).std_unit_system
        except Exception as e:
            actual = None
            log.debug("Could not read the main database's unit system: %s", e)
        if actual is not None and actual != self.db_units:
            log.warning("Main database holds %s data but %s; using the database's units "
                        "(%s) for files without unit_system", unit_system_name(actual),
                        self.db_units_reason, unit_system_name(actual))
            self.db_units, self.db_units_reason = actual, "existing data in the database"
        if self.db_units is None:
            log.warning("Database unit system unknown (%s). Files without unit_system are "
                        "taken to be in the same units as each packet.", self.db_units_reason)
        else:
            log.info("Database unit system: %s (%s). Files without unit_system are taken "
                     "to be in these units.", unit_system_name(self.db_units),
                     self.db_units_reason)
        for s in sources:
            for problem in conversion_problems(s, self.db_units):
                log.warning("Source '%s': %s", s.name, problem)

    def startup(self, _event):
        sources = [r.source for r in self.readers]
        self._check_units(sources)
        try:
            if any((s.missing_columns or self.default_policy) == 'separate' for s in sources):
                ensure_separate_binding(self.config_dict, self.sep_binding)
            plan = build_plan(self._get_manager, sources, self.default_policy,
                              self.main_binding, self.sep_binding)
            added_main, added_sep = apply_plan(self._get_manager, plan,
                                               self.main_binding, self.sep_binding)
        except Exception as e:
            log.error("Could not check/prepare database columns: %s", e)
            return
        self.plan = plan
        if added_main:
            log.info("Added columns to the main archive: %s", ', '.join(added_main))
        if added_sep:
            log.info("Added columns to the separate database: %s", ', '.join(added_sep))
        for line in plan.describe():
            log.info("Columns -> %s", line)
        if plan.unsuitable:
            log.warning("Column(s) %s exist in the main archive but are not numeric",
                        ', '.join(plan.unsuitable))
        if plan.unarchived:
            log.warning("Column(s) %s are injected into loop packets but will NOT be "
                        "archived. Set missing_columns = separate or add.",
                        ', '.join(plan.unarchived))
        unknown = [c for c in plan.main + plan.separate_columns + plan.unarchived
                   if c not in weewx.units.obs_group_dict]
        if unknown:
            log.info("No unit group for %s: stored as read, not converted or labelled in "
                     "reports. Add [%s][[groups]] entries if needed.", ', '.join(unknown),
                     SECTION)

        self.sep_columns = plan.separate_columns
        if self.sep_columns:
            log.info("Separate database: binding '%s', database '%s'", self.sep_binding,
                     self._get_manager(self.sep_binding).database_name)
            # Bound now, not in __init__, so it runs AFTER StdArchive's
            # handler: with hardware record generation StdArchive merges the
            # loop-accumulated values into the record in its own handler.
            self.engine.bind(weewx.NEW_ARCHIVE_RECORD, self.store_separate)

    # -- event handlers --------------------------------------------------

    def _is_day(self, time_ts):
        if self.lat is None:
            return None
        minute = int(time_ts // 60)
        if self._day_cache[0] != minute:
            self._day_cache = (minute, is_daytime(time_ts, self.lat, self.lon, self.altitude_m))
        return self._day_cache[1]

    def _when_ok(self, source, time_ts):
        if source.when == 'always':
            return True
        day = self._is_day(time_ts)
        if day is None:
            return False  # unknown: leave the station's own data alone
        return day if source.when == 'day' else not day

    def _inject(self, target, reader, now, force):
        src = reader.source
        if not self._when_ok(src, target.get('dateTime', now)):
            return 0
        # Loop packets arrive here in the DRIVER's units (StdConvert runs
        # later); archive records in the database's. Either way: file units
        # -> the target's units, and WeeWX does the rest.
        from_system = src.unit_system if src.unit_system is not None else self.db_units
        raw = reader.values(now, force)
        for col in unconvertible_columns(src, self.db_units):
            raw.pop(col, None)          # never store a value in the wrong unit
        values = convert_values(raw, from_system, target.get('usUnits'), src.units)
        count = 0
        for column, value in values.items():
            if not src.overwrite and target.get(column) is not None:
                continue
            target[column] = value
            count += 1
        return count

    def new_loop_packet(self, event):
        now = time.time()
        for reader in self.readers:
            if reader.source.inject_loop:
                try:
                    n = self._inject(event.packet, reader, now, force=False)
                    if n:
                        log.debug("Loop: %d field(s) from '%s'", n, reader.source.name)
                except Exception as e:
                    log.error("Source '%s': loop injection failed: %s", reader.source.name, e)

    def new_archive_record(self, event):
        now = time.time()
        record_ts = event.record.get('dateTime', now)
        for reader in self.readers:
            src = reader.source
            if not src.inject_archive:
                continue
            if src.archive_window and abs(now - record_ts) > src.archive_window:
                log.debug("Source '%s': archive record %s is a catch-up record -- skipped",
                          src.name, timestamp_to_string(record_ts))
                continue
            try:
                n = self._inject(event.record, reader, now, force=True)
                if n:
                    log.debug("Archive: %d field(s) from '%s'", n, src.name)
            except Exception as e:
                log.error("Source '%s': archive injection failed: %s", src.name, e)

    def shutDown(self):
        for fetcher in getattr(self, 'fetchers', {}).values():
            fetcher.stop()

    def store_separate(self, event):
        """Write this period's separate-database columns, after StdArchive
        has stored the main record."""
        record = event.record
        values = {c: record[c] for c in self.sep_columns if record.get(c) is not None}
        if not values or record.get('dateTime') is None:
            return
        out = {'dateTime': record['dateTime'],
               'usUnits': record.get('usUnits'),
               'interval': record.get('interval')}
        out.update(values)
        try:
            mgr = self.engine.db_binder.get_manager(self.sep_binding)
            # Same units as the main archive: whatever the separate database
            # already holds, else the configured database unit system.
            target = mgr.std_unit_system if mgr.std_unit_system is not None \
                else self.db_units
            if target is not None and out['usUnits'] != target:
                out = weewx.units.to_std_system(out, target)
            mgr.addRecord(out, log_success=False)
            log.debug("Separate database: stored %s at %s", ', '.join(sorted(values)),
                      timestamp_to_string(out['dateTime']))
        except Exception as e:
            log.error("Could not store record in separate database: %s", e)


# ---------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------

def cli_fetch(fetcher, force=False):
    """For the command line: use the saved response unless a request is
    due (or force -- which still obeys max_requests_per_day and Retry-After).
    Returns (text or None, message)."""
    wait, why = fetcher.wait_time(force=force)
    if wait > 0:
        text, at = fetcher.latest()
        if text is not None:
            return text, "using saved response from %.0f s ago; next request allowed in " \
                         "%.0f s (%s)" % (time.time() - at, wait, why)
        return None, "no saved response and no request allowed for %.0f s (%s)" % (wait, why)
    try:
        text = fetcher.fetch_once(force=True)
        return text, "fetched now (%d characters)" % len(text)
    except RateLimited as e:
        return fetcher.latest()[0], str(e)
    except Exception as e:
        text = fetcher.latest()[0]
        return text, "fetch failed: %s%s" % (scrub(e, fetcher.spec.secrets),
                                              "; using saved response" if text else "")


class _ManagerPool(object):
    """get_manager() for the command line: opens each binding once."""

    def __init__(self, config_dict, create=True):
        self.config_dict = config_dict
        self.create = create
        self.managers = {}

    def __call__(self, binding):
        import weewx.manager
        if binding not in self.managers:
            self.managers[binding] = weewx.manager.open_manager_with_config(
                self.config_dict, binding, initialize=self.create)
        return self.managers[binding]

    def close(self):
        for mgr in self.managers.values():
            mgr.close()


def schema_types(config_dict, binding):
    """{column: SQL type} from a binding's schema, or None."""
    import weewx.manager
    try:
        sch = weewx.manager.get_manager_dict_from_config(config_dict, binding)['schema']
    except Exception:
        return None
    if isinstance(sch, dict):
        sch = sch.get('table', [])
    return {name: sql.split()[0] for name, sql in (sch or [])}


def _policy_and_bindings(config_dict):
    section = service_section(config_dict)
    policy = _choice(section.get('missing_columns'), MISSING_CHOICES, 'separate',
                     'missing_columns', SECTION)
    return (policy, section.get('data_binding', MAIN_BINDING),
            section.get('separate_binding', SEPARATE_BINDING))


def _cmd_test(config_dict, force_fetch=False):
    sources = load_sources(config_dict)
    if not sources:
        print("No sources configured in [%s][[sources]]." % SECTION)
        return 1
    policy, main_b, sep_b = _policy_and_bindings(config_dict)
    ensure_separate_binding(config_dict, sep_b)
    db_units, reason = configured_db_units(config_dict)
    pool = _ManagerPool(config_dict, create=False)
    plan = None
    try:
        try:
            main_types = column_types(pool(main_b))
            actual = pool(main_b).std_unit_system
            if actual is not None and actual != db_units:
                print("** Main database holds %s data but %s -- using %s **"
                      % (unit_system_name(actual), reason, unit_system_name(actual)))
                db_units, reason = actual, "existing data in the database"
        except Exception as e:
            # Not created yet (WeeWX never run): use the binding's schema.
            main_types = schema_types(config_dict, main_b)
            print("(main database not opened: %s -- using its schema)" % e)
        try:
            sep_cols = pool(sep_b).sqlkeys
        except Exception:
            sep_cols = []       # separate database not created yet
        if main_types is not None:
            plan = ColumnPlan(wanted_columns(sources, policy), main_types, sep_cols)
    finally:
        pool.close()

    where = {}
    if plan:
        for col in plan.main:
            where[col] = 'main'
        for col in plan.add_main:
            where[col] = 'main (add)'
        for col in plan.separate:
            where[col] = 'separate'
        for col in plan.add_sep:
            where[col] = 'separate (add)'
        for col in plan.unarchived:
            where[col] = 'NOT ARCHIVED'

    print("Database unit system: %s (%s)" % (unit_system_name(db_units), reason))
    placeholders = station_placeholders(config_dict)
    cache_dir, fetchers = cache_dir_for(config_dict), {}
    for s in sources:
        print("\n[%s]" % s.name)
        if not s.enable:
            print("  path: %s" % (redact_url(s.path) if s.is_url else s.path))
            print("  DISABLED")
            continue
        try:
            reader = make_reader(s, placeholders, cache_dir, fetchers)
        except ValueError as e:
            print("  ** %s **" % e)
            continue
        if reader.fetcher is not None:
            f = reader.fetcher
            print("  url:  %s" % redact_url(f.url))
            if f.source is not s:
                print("  (same URL as '%s': shares its requests)" % f.source.name)
            else:
                print("  limits: %s" % f.status())
                text, msg = cli_fetch(f, force_fetch)
                print("  %s" % msg)
                if text is None:
                    continue
        else:
            print("  path: %s" % s.path)
        now = time.time()
        data, age = reader.read(now)
        if age is not None:
            print("  age: %.0f s%s" % (age, '  ** older than max_age %d **' % s.max_age
                                       if data is None else ''))
        if data is None and age is None:
            print("  ** could not read (see messages above) **")
            continue
        file_units = s.unit_system if s.unit_system is not None else db_units
        print("  inject: loop=%s archive=%s when=%s overwrite=%s" % (
            s.inject_loop, s.inject_archive, s.when, s.overwrite))
        print("  file units: %s%s" % (
            unit_system_name(file_units) if s.unit_system is not None
            else "%s (same as database)" % unit_system_name(db_units),
            ''.join("; %s in %s" % (c, u) for c, u in s.units.items())))
        for problem in conversion_problems(s, db_units):
            print("  ** %s **" % problem)
        values = reader.values(now, force=True) if data is not None else {}
        blocked = unconvertible_columns(s, db_units)
        stored = convert_values({c: v for c, v in values.items() if c not in blocked},
                                file_units, db_units, s.units)
        for key, col in s.mapping.items():
            found, raw = lookup(data, key) if data is not None else (False, None)
            if col in values:
                status = "%-12s" % values[col]
                if col in blocked:
                    status += " -> NOT STORED (no unit group)"
                elif col not in stored:
                    status += " -> NOT STORED (cannot convert)"
                elif stored[col] != values[col]:
                    unit = weewx.units.getStandardUnitType(db_units, col)[0]
                    status += " -> %.4g %s" % (stored[col], unit)
            elif found:
                status = "not numeric (%r)" % (raw,)
            else:
                status = "key not found"
            print("  %-28s -> %-22s %-28s [%s]" % (key, col, status, where.get(col, '?')))
    if plan:
        print("\nColumns:")
        for line in plan.describe():
            print("  " + line)
        if plan.add_main or plan.add_sep:
            print("\nRun 'setup-columns' to create them now, or let the service do it "
                  "when WeeWX starts.")
    return 0


def _cmd_paths(config_dict, target, limit, force_fetch=False):
    """Show every path in a source's file or URL (or any file or URL), for
    writing mappings."""
    source = None
    for s in load_sources(config_dict):
        if s.name == target:
            source = s
            break
    if source is None:
        source = SourceConfig('(command line)', target, {})
    json_path = source.json_path
    try:
        configured = source.name != '(command line)'
        reader = make_reader(source, station_placeholders(config_dict),
                             cache_dir_for(config_dict) if configured else None)
        if reader.fetcher is not None:
            shown = redact_url(reader.fetcher.url)
            if configured:
                text, msg = cli_fetch(reader.fetcher, force_fetch)
                print(msg)
                if text is None:
                    return 1
            else:
                text = reader.fetcher.fetch_once()     # a one-off URL you typed
        else:
            shown = source.path
            with open(source.path, 'r', encoding='utf-8-sig') as f:
                text = f.read()
        data = parse_text(text, source.format)
    except Exception as e:
        print("Could not read %s: %s" % (redact_url(source.path) if source.is_url
                                         else source.path, e))
        return 1
    print("Source: %s" % shown)
    if json_path:
        ok, data = lookup(data, json_path)
        print("Relative to json_path '%s'%s" % (json_path, '' if ok else ' -- NOT FOUND'))
        if not ok:
            return 1
    count = 0
    for p, value in flatten(data):
        number = to_number(value)
        print("  %-50s %r%s" % (p, value, '' if number is not None else '   (text)'))
        count += 1
        if limit and count >= limit:
            print("  ... (more; use --limit 0 for all)")
            break
    return 0


def _cmd_setup_columns(config_dict, assume_yes):
    sources = load_sources(config_dict)
    policy, main_b, sep_b = _policy_and_bindings(config_dict)
    if any((s.missing_columns or policy) == 'separate' for s in sources):
        ensure_separate_binding(config_dict, sep_b)
    pool = _ManagerPool(config_dict, create=True)
    try:
        plan = build_plan(pool, sources, policy, main_b, sep_b)
        for line in plan.describe():
            print(line)
        if not (plan.add_main or plan.add_sep):
            print("Nothing to add.")
            return 0
        if plan.add_main and not assume_yes:
            if input("Columns will be added to the MAIN archive. Stop WeeWX and back up "
                     "the database first. Proceed? [y/N] ").strip().lower() not in ('y', 'yes'):
                print("Nothing done.")
                return 1
        added_main, added_sep = apply_plan(pool, plan, main_b, sep_b)
        if added_main:
            print("Added to main archive: %s" % ', '.join(added_main))
        if added_sep:
            print("Added to separate database (%s): %s"
                  % (pool(sep_b).database_name, ', '.join(added_sep)))
        return 0
    finally:
        pool.close()


def main(argv=None):
    import argparse
    import weecfg
    import weeutil.logger

    parser = argparse.ArgumentParser(
        prog='python3 -m user.dataingest',
        description="weewx-data-ingest %s helper" % VERSION)
    parser.add_argument('--config', help="Path to weewx.conf")
    sub = parser.add_subparsers(dest='command')
    p_test = sub.add_parser('test', help="Read every source; show values and where each "
                                         "column will be archived (default)")
    p_test.add_argument('--fetch', action='store_true', help='Request URL sources now even if poll_interval has not passed (the daily cap and server Retry-After still apply)')
    p_paths = sub.add_parser('paths', help="List every path in a source's file or URL, for "
                                           "writing mappings")
    p_paths.add_argument('source', help="Source name, or any JSON/XML/text file or URL")
    p_paths.add_argument('--limit', type=int, default=200, help="Max lines (0 = all)")
    p_paths.add_argument('--fetch', action='store_true', help='Request URL sources now even if poll_interval has not passed (the daily cap and server Retry-After still apply)')
    p_setup = sub.add_parser('setup-columns', help="Create missing columns now "
                                                   "(main archive and/or separate database)")
    p_setup.add_argument('-y', '--yes', action='store_true',
                         help="Don't ask for confirmation")
    args = parser.parse_args(argv)

    config_path, config_dict = weecfg.read_config(args.config)
    weeutil.logger.setup('dataingest', config_dict)
    logging.getLogger().addHandler(logging.StreamHandler())
    print("Using %s" % config_path)
    register_groups(service_section(config_dict))

    if args.command == 'paths':
        return _cmd_paths(config_dict, args.source, args.limit, args.fetch)
    if args.command == 'setup-columns':
        return _cmd_setup_columns(config_dict, args.yes)
    return _cmd_test(config_dict, getattr(args, 'fetch', False))


if __name__ == '__main__':
    import sys
    sys.exit(main())
