"""Unit tests for weewx-data-ingest.  Run:  python3 -m pytest tests"""
import json
import os
import sys
import time

import configobj
import pytest
import weewx
import weewx.accum
import weewx.manager
import weewx.units

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'bin'))
from user import dataingest as da  # noqa: E402


# ---------------------------------------------------------------- helpers

class FakeStnInfo(object):
    latitude_f = 51.5
    longitude_f = -0.12
    altitude_vt = weewx.units.ValueTuple(20, 'meter', 'group_altitude')


class FakeEngine(object):
    """Enough of StdEngine: bind(), stn_info and a real DBBinder."""

    def __init__(self, cfg):
        self.stn_info = FakeStnInfo()
        self.callbacks = {}
        self.db_binder = weewx.manager.DBBinder(cfg)

    def bind(self, event_type, callback):
        self.callbacks.setdefault(event_type, []).append(callback)


class Event(object):
    def __init__(self, **kw):
        self.__dict__.update(kw)


BASE = """
WEEWX_ROOT = %(root)s
[DataBindings]
    [[wx_binding]]
        database = archive_sqlite
        table_name = archive
        manager = weewx.manager.DaySummaryManager
        schema = weewx.schemas.wview_extended.schema
[Databases]
    [[archive_sqlite]]
        database_name = weewx.sdb
        database_type = SQLite
[DatabaseTypes]
    [[SQLite]]
        driver = weedb.sqlite
        SQLITE_ROOT = %(db)s
"""


def make_config(tmp_path, text):
    db_dir = tmp_path / 'db'
    db_dir.mkdir(exist_ok=True)
    base = BASE % {'root': tmp_path, 'db': db_dir}
    return configobj.ConfigObj((base + text).splitlines(), interpolation=False)


def write_json(path, obj, age=0):
    path.write_text(json.dumps(obj))
    if age:
        t = time.time() - age
        os.utime(path, (t, t))


def make_service(cfg):
    engine = FakeEngine(cfg)
    return da.DataIngestService(engine, cfg), engine


def start(svc, engine):
    for cb in list(engine.callbacks.get(weewx.STARTUP, [])):
        cb(Event())


def open_db(cfg, binding):
    return weewx.manager.open_manager_with_config(cfg, binding)


# ---------------------------------------------------------------- parsing

def test_parse_mapping_styles():
    assert da.parse_mapping({'a': 'x', 'b': 'y'}) == {'a': 'x', 'b': 'y'}
    assert da.parse_mapping('a:x') == {'a': 'x'}
    assert da.parse_mapping(['a:x', ' b : y ']) == {'a': 'x', 'b': 'y'}
    assert da.parse_mapping('a:x|b:y') == {'a': 'x', 'b': 'y'}
    assert da.parse_mapping(['nocolon']) == {}


def test_validate_mapping_rejects_reserved_and_bad_names():
    m = {'a': 'dateTime', 'b': 'bad name', 'c': 'drop;table', 'd': 'ok_col1'}
    assert da.validate_mapping(m, 't') == {'d': 'ok_col1'}


def test_parse_text_auto_detects_format():
    assert da.parse_text('{"a": 1}') == {'a': 1}
    assert da.parse_text('﻿  [1, 2]') == [1, 2]
    assert da.parse_text('# c\na = 1\nb: 2\nc,3\n') == {'a': '1', 'b': '2', 'c': '3'}
    assert da.parse_text('<?xml version="1.0" encoding="UTF-8"?><r><v>3</v></r>') == \
        {'r': {'v': '3'}}


def test_parse_xml_structure():
    xml = """<?xml version="1.0"?>
    <ns:status xmlns:ns="http://example.com/x" updated="2026-10-21">
      <site id="SUM" level="2">amber</site>
      <reading><t>1.5</t></reading>
      <reading><t>2.5</t></reading>
      <reading><t>3.5</t></reading>
    </ns:status>"""
    data = da.parse_xml(xml)
    st = data['status']
    assert st['@updated'] == '2026-10-21'
    assert st['site'] == {'@id': 'SUM', '@level': '2', '#text': 'amber'}
    assert [r['t'] for r in st['reading']] == ['1.5', '2.5', '3.5']
    assert da.lookup(data, 'status.reading.-1.t') == (True, '3.5')
    assert da.lookup(data, 'status.site.@level') == (True, '2')


def test_parse_xml_error_is_valueerror():
    with pytest.raises(ValueError):
        da.parse_text('<a><b></a>', 'xml')


def test_lookup_paths():
    data = {'current': {'t': 5}, 'hourly': {'t': [1, 2, 3]}}
    assert da.lookup(data, 'current.t') == (True, 5)
    assert da.lookup(data, 'hourly.t.2') == (True, 3)
    assert da.lookup(data, 'hourly.t.-1') == (True, 3)
    assert da.lookup(data, 'hourly.t.9') == (False, None)
    assert da.lookup(data, 'nope') == (False, None)
    # Single XML element addressed as if it repeated.
    assert da.lookup({'e': {'v': 1}}, 'e.0.v') == (True, 1)
    assert da.lookup({'e': {'v': 1}}, 'e.-1.v') == (True, 1)


@pytest.mark.parametrize('raw,expected', [
    (1, 1.0), ('2.5', 2.5), (None, None), ('N/A', None), ('', None),
    (True, 1.0), ({'a': 1}, None), ({'#text': '4'}, 4.0), (float('nan'), None),
    ([1], None)])
def test_to_number(raw, expected):
    assert da.to_number(raw) == expected


def test_to_number_text_values():
    tv = {'green': 0.0, 'amber': 2.0}
    assert da.to_number(' Amber ', tv) == 2.0
    assert da.to_number({'#text': 'green'}, tv) == 0.0
    assert da.to_number('purple', tv) is None


def test_flatten():
    assert dict(da.flatten({'a': {'b': [1, {'c': 2}]}})) == {'a.b.0': 1, 'a.b.1.c': 2}


# ---------------------------------------------------------------- config

def test_section_defaults_apply_to_every_source(tmp_path):
    cfg = make_config(tmp_path, """
[DataIngest]
    max_age = 120
    when = day
    [[sources]]
        [[[a]]]
            path = /x/a.json
            mapping = o:o3
        [[[b]]]
            path = /x/b.json
            max_age = 30
            mapping = n:no2
        [[[c]]]
            path = disable
            mapping = n:no3
""")
    srcs = {s.name: s for s in da.load_sources(cfg)}
    assert srcs['a'].max_age == 120 and srcs['a'].when == 'day'
    assert srcs['b'].max_age == 30
    assert not srcs['c'].enable


def test_other_sections_are_not_read(tmp_path):
    cfg = make_config(tmp_path, """
[DataInjectService]
    [[source_x]]
        path = /x
        mapping = a:b
""")
    assert da.load_sources(cfg) == []







# ---------------------------------------------------------------- column plan

def test_column_plan_routing():
    main = {'outTemp': 'REAL', 'pm2_5': 'REAL', 'label': 'STR'}
    wanted = {'outTemp': 'separate', 'o3': 'separate', 'no2': 'separate',
              'newcol': 'add', 'ignored': 'warn', 'label': 'separate'}
    plan = da.ColumnPlan(wanted, main, sep_columns={'dateTime', 'o3'})
    assert plan.main == ['outTemp']
    assert plan.separate == ['o3']
    assert plan.add_sep == ['no2', 'label']
    assert plan.add_main == ['newcol']
    assert plan.unarchived == ['ignored']
    assert plan.unsuitable == ['label']


def test_wanted_columns_strongest_policy_wins():
    a = da.SourceConfig('a', '/a', {'k': 'x'}, missing_columns='warn')
    b = da.SourceConfig('b', '/b', {'k': 'x'}, missing_columns='add')
    c = da.SourceConfig('c', '/c', {'k': 'y'})
    assert da.wanted_columns([a, b, c], 'separate') == {'x': 'add', 'y': 'separate'}


# ---------------------------------------------------------------- service

def test_loop_and_archive_injection(tmp_path):
    src = tmp_path / 'aq.txt'
    write_json(src, {'current': {'ozone': 41.2, 'dust': None, 'pm': 'n/a'}})
    cfg = make_config(tmp_path, """
[DataIngest]
    [[sources]]
        [[[aq]]]
            path = %s
            json_path = current
            [[[[mapping]]]]
                ozone = o3
                dust = dust
                pm = pm2_5
""" % src)
    svc, engine = make_service(cfg)
    assert weewx.NEW_LOOP_PACKET in engine.callbacks
    assert weewx.NEW_ARCHIVE_RECORD in engine.callbacks

    packet = {'dateTime': time.time(), 'usUnits': weewx.METRIC, 'o3': 1.0}
    svc.new_loop_packet(Event(packet=packet))
    assert packet['o3'] == 41.2
    assert 'dust' not in packet and 'pm2_5' not in packet

    record = {'dateTime': time.time(), 'usUnits': weewx.METRIC}
    svc.new_archive_record(Event(record=record))
    assert record['o3'] == 41.2


def test_xml_source_with_text_values(tmp_path):
    src = tmp_path / 'aurora.xml'
    src.write_text("""<?xml version="1.0" encoding="UTF-8"?>
<current_status><site_status status_id="amber"/>
  <activity><value>12.1</value></activity>
  <activity><value>48.6</value></activity>
</current_status>""")
    cfg = make_config(tmp_path, """
[DataIngest]
    [[sources]]
        [[[aurora]]]
            path = %s
            json_path = current_status
            [[[[mapping]]]]
                activity.-1.value = auroraActivity
                site_status.@status_id = auroraStatus
            [[[[text_values]]]]
                green = 0
                yellow = 1
                amber = 2
                red = 3
""" % src)
    svc, _ = make_service(cfg)
    p = {'dateTime': time.time(), 'usUnits': weewx.METRIC}
    svc.new_loop_packet(Event(packet=p))
    assert p['auroraActivity'] == 48.6
    assert p['auroraStatus'] == 2.0


def test_keyvalue_source(tmp_path):
    src = tmp_path / 'clientraw.txt'
    src.write_text("# sensor dump\nsoil = 21.5\nleaf: 3\n")
    cfg = make_config(tmp_path, """
[DataIngest]
    [[sources]]
        [[[soil]]]
            path = %s
            unit_system = METRIC
            mapping = soil:soilTemp1, leaf:leafWet1
""" % src)
    svc, _ = make_service(cfg)
    p = {'dateTime': time.time(), 'usUnits': weewx.METRIC}
    svc.new_loop_packet(Event(packet=p))
    assert p['soilTemp1'] == 21.5 and p['leafWet1'] == 3.0


def test_reread_only_on_change_and_keep_last_good(tmp_path):
    src = tmp_path / 'a.json'
    write_json(src, {'v': 1})
    cfg = make_config(tmp_path, """
[DataIngest]
    [[sources]]
        [[[a]]]
            path = %s
            mapping = v:extraTemp1
""" % src)
    svc, _ = make_service(cfg)
    p = {'dateTime': time.time(), 'usUnits': weewx.US}
    svc.new_loop_packet(Event(packet=p))
    assert p['extraTemp1'] == 1.0

    # Half-written file: the previous value is still used.
    src.write_text('{"v": ')
    os.utime(src, (time.time() + 1, time.time() + 1))
    p = {'dateTime': time.time(), 'usUnits': weewx.US}
    svc.new_loop_packet(Event(packet=p))
    assert p['extraTemp1'] == 1.0

    src.write_text('{"v": 7}')
    os.utime(src, (time.time() + 2, time.time() + 2))
    p = {'dateTime': time.time(), 'usUnits': weewx.US}
    svc.new_loop_packet(Event(packet=p))
    assert p['extraTemp1'] == 7.0


def test_max_age_blocks_stale_data(tmp_path):
    src = tmp_path / 'a.json'
    write_json(src, {'v': 1}, age=1000)
    cfg = make_config(tmp_path, """
[DataIngest]
    [[sources]]
        [[[a]]]
            path = %s
            max_age = 600
            mapping = v:extraTemp1
""" % src)
    svc, _ = make_service(cfg)
    p = {'dateTime': time.time(), 'usUnits': weewx.US}
    svc.new_loop_packet(Event(packet=p))
    assert 'extraTemp1' not in p


def test_unit_conversion_and_overwrite_false(tmp_path):
    src = tmp_path / 'a.json'
    write_json(src, {'t': 20.0, 'h': 50})
    cfg = make_config(tmp_path, """
[DataIngest]
    [[sources]]
        [[[a]]]
            path = %s
            unit_system = METRIC
            overwrite = false
            mapping = t:extraTemp1, h:extraHumid1
""" % src)
    svc, _ = make_service(cfg)
    p = {'dateTime': time.time(), 'usUnits': weewx.US, 'extraHumid1': 40.0}
    svc.new_loop_packet(Event(packet=p))
    assert p['extraTemp1'] == pytest.approx(68.0)
    assert p['extraHumid1'] == 40.0          # station value kept


def test_catchup_records_are_not_injected(tmp_path):
    src = tmp_path / 'a.json'
    write_json(src, {'v': 3})
    cfg = make_config(tmp_path, """
[DataIngest]
    [[sources]]
        [[[a]]]
            path = %s
            mapping = v:extraTemp1
""" % src)
    svc, _ = make_service(cfg)
    old = {'dateTime': time.time() - 7200, 'usUnits': weewx.US}
    svc.new_archive_record(Event(record=old))
    assert 'extraTemp1' not in old


def test_extractor_registered_for_stdarchive(tmp_path):
    src = tmp_path / 'a.json'
    write_json(src, {'kp': 3})
    cfg = make_config(tmp_path, """
[Accumulator]
    [[userCol]]
        extractor = max
[DataIngest]
    [[sources]]
        [[[a]]]
            path = %s
            extractor = last
            mapping = kp:kpIndex, x:userCol
""" % src)
    make_service(cfg)
    assert cfg['Accumulator']['kpIndex'] == {'extractor': 'last'}
    assert cfg['Accumulator']['userCol']['extractor'] == 'max'   # user's wins


def test_groups_registered(tmp_path):
    src = tmp_path / 'a.json'
    write_json(src, {'o': 1})
    cfg = make_config(tmp_path, """
[DataIngest]
    [[groups]]
        ozoneTest = group_concentration
        badGroup = concentration
    [[sources]]
        [[[a]]]
            path = %s
            mapping = o:ozoneTest
""" % src)
    make_service(cfg)
    assert weewx.units.obs_group_dict['ozoneTest'] == 'group_concentration'
    assert 'badGroup' not in weewx.units.obs_group_dict


# ------------------------------------------------- startup: database columns

def _src_cfg(tmp_path, policy_line='', extra=''):
    src = tmp_path / 'aq.json'
    write_json(src, {'ozone': 41.2, 'no2': 12.0, 'out': 15.0})
    return make_config(tmp_path, """
[DataIngest]
    %s
    [[sources]]
        [[[aq]]]
            path = %s
            %s
            [[[[mapping]]]]
                ozone = o3Test
                no2 = no2Test
                out = extraTemp1
""" % (policy_line, src, extra))


def test_default_policy_creates_separate_database(tmp_path):
    cfg = _src_cfg(tmp_path)
    svc, engine = make_service(cfg)
    start(svc, engine)

    # Main archive untouched; separate DB created with the new columns.
    with open_db(cfg, 'wx_binding') as main:
        assert 'o3Test' not in main.sqlkeys
    assert (tmp_path / 'db' / 'dataingest.sdb').exists()
    with open_db(cfg, 'dataingest_binding') as sep:
        assert {'o3Test', 'no2Test'} <= set(sep.sqlkeys)
        assert 'extraTemp1' not in sep.sqlkeys    # already in main archive
        assert {'o3Test', 'no2Test'} <= sep.daykeys
    assert svc.plan.main == ['extraTemp1']
    assert sorted(svc.sep_columns) == ['no2Test', 'o3Test']

    # store_separate must run after the archive service's handler.
    handlers = engine.callbacks[weewx.NEW_ARCHIVE_RECORD]
    assert handlers[-1] == svc.store_separate
    assert handlers.index(svc.new_archive_record) < handlers.index(svc.store_separate)


def test_separate_database_receives_archive_records(tmp_path):
    cfg = _src_cfg(tmp_path)
    svc, engine = make_service(cfg)
    start(svc, engine)

    ts = int(time.time() // 300 * 300)
    rec = {'dateTime': ts, 'usUnits': weewx.METRIC, 'interval': 5, 'outTemp': 10.0}
    ev = Event(record=rec)
    for cb in engine.callbacks[weewx.NEW_ARCHIVE_RECORD]:
        cb(ev)
    # A record with none of the separate columns writes nothing.
    for cb in engine.callbacks[weewx.NEW_ARCHIVE_RECORD]:
        cb(Event(record={'dateTime': ts - 7200, 'usUnits': weewx.METRIC, 'interval': 5}))

    engine.db_binder.close()
    with open_db(cfg, 'dataingest_binding') as sep:
        rows = list(sep.genBatchRecords())
        assert len(rows) == 1
        assert rows[0]['dateTime'] == ts
        assert rows[0]['o3Test'] == 41.2 and rows[0]['no2Test'] == 12.0
        assert 'outTemp' not in rows[0]
        assert sep.getSql("SELECT max FROM archive_day_o3Test")[0] == 41.2


def test_separate_database_unit_mismatch_converted(tmp_path):
    cfg = _src_cfg(tmp_path)
    cfg['DataIngest']['groups'] = {'no2Test': 'group_temperature'}   # just to force a conversion
    svc, engine = make_service(cfg)
    start(svc, engine)
    ts = int(time.time() // 300 * 300)
    svc.store_separate(Event(record={'dateTime': ts - 300, 'usUnits': weewx.METRIC,
                                     'interval': 5, 'no2Test': 0.0}))
    svc.store_separate(Event(record={'dateTime': ts, 'usUnits': weewx.US,
                                     'interval': 5, 'no2Test': 212.0}))
    engine.db_binder.close()
    with open_db(cfg, 'dataingest_binding') as sep:
        vals = [r['no2Test'] for r in sep.genBatchRecords()]
    # Empty separate database: takes the configured database units (US).
    assert vals == [pytest.approx(32.0), 212.0]
    weewx.units.obs_group_dict.pop('no2Test', None)


def test_separate_columns_added_on_later_startup(tmp_path):
    cfg = _src_cfg(tmp_path)
    svc, engine = make_service(cfg)
    start(svc, engine)
    engine.db_binder.close()

    cfg['DataIngest']['sources']['aq']['mapping']['pm'] = 'pmTest'
    svc, engine = make_service(cfg)
    start(svc, engine)
    engine.db_binder.close()
    with open_db(cfg, 'dataingest_binding') as sep:
        assert {'o3Test', 'no2Test', 'pmTest'} <= set(sep.sqlkeys)


def test_add_policy_adds_to_main_archive(tmp_path):
    cfg = _src_cfg(tmp_path, 'missing_columns = add')
    svc, engine = make_service(cfg)
    start(svc, engine)
    # The cached manager StdArchive will use already knows the new columns.
    cached = engine.db_binder.get_manager('wx_binding')
    assert {'o3Test', 'no2Test'} <= set(cached.sqlkeys)
    assert 'o3Test' in cached.daykeys
    assert not (tmp_path / 'db' / 'dataingest.sdb').exists()
    assert weewx.NEW_ARCHIVE_RECORD in engine.callbacks
    assert svc.store_separate not in engine.callbacks[weewx.NEW_ARCHIVE_RECORD]

    rec = {'dateTime': int(time.time()), 'usUnits': weewx.US, 'interval': 5}
    svc.new_archive_record(Event(record=rec))
    cached.addRecord(rec)
    assert cached.getSql("SELECT o3Test FROM archive")[0] == 41.2




def test_warn_policy_changes_nothing(tmp_path):
    cfg = _src_cfg(tmp_path, 'missing_columns = warn')
    svc, engine = make_service(cfg)
    start(svc, engine)
    assert svc.plan.unarchived == ['o3Test', 'no2Test']
    with open_db(cfg, 'wx_binding') as main:
        assert 'o3Test' not in main.sqlkeys
    assert not (tmp_path / 'db' / 'dataingest.sdb').exists()


def test_per_source_policy_override(tmp_path):
    cfg = _src_cfg(tmp_path, 'missing_columns = warn', 'missing_columns = separate')
    svc, engine = make_service(cfg)
    start(svc, engine)
    assert sorted(svc.sep_columns) == ['no2Test', 'o3Test']


def test_configured_db_units():
    assert da.configured_db_units({})[0] == weewx.US
    assert da.configured_db_units({'StdConvert': {'target_unit': 'metricwx'}})[0] == \
        weewx.METRICWX
    no_convert = {'Engine': {'Services': {'process_services': ['weewx.engine.StdCalibrate']}},
                  'StdConvert': {'target_unit': 'METRIC'}}
    assert da.configured_db_units(no_convert)[0] is None
    with_convert = {'Engine': {'Services': {
        'process_services': 'weewx.engine.StdConvert, weewx.engine.StdQC'}},
        'StdConvert': {'target_unit': 'METRIC'}}
    assert da.configured_db_units(with_convert)[0] == weewx.METRIC


def _temp_source(tmp_path, value, extra='', conf=''):
    src = tmp_path / 't.json'
    write_json(src, {'t': value})
    return make_config(tmp_path, conf + """
[DataIngest]
    [[sources]]
        [[[t]]]
            path = %s
            mapping = t:extraTemp1
            %s
""" % (src, extra))


def test_file_without_unit_system_is_in_database_units(tmp_path):
    """Loop packets (driver units, here METRIC) and archive records
    (database units, US) must both end up with the same temperature."""
    cfg = _temp_source(tmp_path, 68.0)              # database default: US
    svc, _ = make_service(cfg)
    loop = {'dateTime': time.time(), 'usUnits': weewx.METRIC}
    svc.new_loop_packet(Event(packet=loop))
    assert loop['extraTemp1'] == pytest.approx(20.0)
    rec = {'dateTime': time.time(), 'usUnits': weewx.US}
    svc.new_archive_record(Event(record=rec))
    assert rec['extraTemp1'] == pytest.approx(68.0)


def test_metric_database_from_stdconvert(tmp_path):
    cfg = _temp_source(tmp_path, 20.0, conf="[StdConvert]\n    target_unit = METRIC\n")
    svc, _ = make_service(cfg)
    assert svc.db_units == weewx.METRIC
    loop = {'dateTime': time.time(), 'usUnits': weewx.US}
    svc.new_loop_packet(Event(packet=loop))
    assert loop['extraTemp1'] == pytest.approx(68.0)   # StdConvert turns it back to 20


def test_per_column_units(tmp_path):
    src = tmp_path / 'w.json'
    write_json(src, {'t': 20.0, 'w': 36.0, 'p': 1013.25})
    cfg = make_config(tmp_path, """
[DataIngest]
    [[sources]]
        [[[w]]]
            path = %s
            unit_system = US
            mapping = t:extraTemp1, w:windSpeed, p:pressure
            [[[[units]]]]
                extraTemp1 = degree_C
                windSpeed = km_per_hour
                notMapped = degree_C
""" % src)
    svc, _ = make_service(cfg)
    s = svc.readers[0].source
    assert s.units == {'extraTemp1': 'degree_C', 'windSpeed': 'km_per_hour'}
    p = {'dateTime': time.time(), 'usUnits': weewx.METRICWX}
    svc.new_loop_packet(Event(packet=p))
    assert p['extraTemp1'] == pytest.approx(20.0)
    assert p['windSpeed'] == pytest.approx(10.0)          # km/h -> m/s
    assert p['pressure'] == pytest.approx(1013.25 * 33.8639, rel=1e-3)  # US inHg -> mbar


def test_bad_and_incompatible_units(tmp_path):
    cfg = _temp_source(tmp_path, 20.0, """[[[[units]]]]
                extraTemp1 = furlongs""")
    svc, _ = make_service(cfg)
    assert svc.readers[0].source.units == {}            # unknown unit name ignored

    s = da.SourceConfig('x', '/x', {'t': 'extraTemp1'}, units={'extraTemp1': 'km_per_hour'})
    problems = da.conversion_problems(s, weewx.US)
    assert problems and 'cannot convert' in problems[0]
    assert da.convert_values({'extraTemp1': 5.0}, None, weewx.US, s.units) == {}


def test_column_without_group_is_reported(tmp_path):
    s = da.SourceConfig('x', '/x', {'v': 'mysteryCol'}, unit_system=weewx.METRIC)
    assert 'no unit group' in da.conversion_problems(s, weewx.US)[0]
    assert da.convert_values({'mysteryCol': 5.0}, weewx.METRIC, weewx.US) == {'mysteryCol': 5.0}


def test_existing_database_units_win(tmp_path):
    cfg = _temp_source(tmp_path, 20.0)              # configured: US (default)
    with weewx.manager.open_manager_with_config(cfg, 'wx_binding', initialize=True) as m:
        m.addRecord({'dateTime': 1700000000, 'usUnits': weewx.METRIC, 'interval': 5,
                     'outTemp': 10.0})
    svc, engine = make_service(cfg)
    start(svc, engine)
    assert svc.db_units == weewx.METRIC


def test_custom_separate_binding_mysql_style_name(tmp_path):
    """A user-defined separate binding in weewx.conf is used as-is."""
    cfg = _src_cfg(tmp_path, 'separate_binding = extra_binding')
    cfg['DataBindings']['extra_binding'] = {
        'database': 'extra_sqlite', 'table_name': 'extras',
        'manager': 'weewx.manager.DaySummaryManager', 'schema': 'user.dataingest.schema'}
    cfg['Databases']['extra_sqlite'] = {'database_name': 'extras.sdb',
                                        'database_type': 'SQLite'}
    svc, engine = make_service(cfg)
    start(svc, engine)
    engine.db_binder.close()
    with open_db(cfg, 'extra_binding') as sep:
        assert sep.table_name == 'extras'
        assert 'o3Test' in sep.sqlkeys


def test_startup_failure_does_not_raise(tmp_path):
    cfg = _src_cfg(tmp_path, 'data_binding = no_such_binding')
    svc, engine = make_service(cfg)
    start(svc, engine)          # logged, not raised
    assert svc.plan is None


# ---------------------------------------------------------------- CLI

def _write_conf(tmp_path, cfg):
    path = tmp_path / 'weewx.conf'
    cfg.filename = str(path)
    cfg.write()
    return str(path)


def test_cli_test_and_setup(tmp_path, capsys):
    cfg = _src_cfg(tmp_path)
    conf = _write_conf(tmp_path, cfg)
    weewx.manager.open_manager_with_config(cfg, 'wx_binding', initialize=True).close()

    assert da.main(['--config', conf, 'test']) == 0
    out = capsys.readouterr().out
    assert 'separate (add)' in out and '41.2' in out

    assert da.main(['--config', conf, 'setup-columns', '-y']) == 0
    out = capsys.readouterr().out
    assert 'Added to separate database' in out
    assert da.main(['--config', conf, 'test']) == 0
    assert '[separate]' in capsys.readouterr().out


def test_cli_paths(tmp_path, capsys):
    cfg = _src_cfg(tmp_path)
    conf = _write_conf(tmp_path, cfg)
    assert da.main(['--config', conf, 'paths', 'aq']) == 0
    out = capsys.readouterr().out
    assert 'ozone' in out and '41.2' in out


# ---------------------------------------------------------------- installer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import install  # noqa: E402


class _Printer(object):
    def __init__(self):
        self.lines = []

    def out(self, msg, level=0):
        self.lines.append(msg)


def _inst_engine(extra='', services=None):
    text = """
[DataBindings]
    [[wx_binding]]
        database = archive_sqlite
[Databases]
    [[archive_sqlite]]
        database_name = weewx.sdb
        database_type = SQLite
    [[archive_mysql]]
        database_name = weewx
        database_type = MySQL
[Engine]
    [[Services]]
        data_services = %s
""" % (services or 'user.mqtt.LoopPublisher, user.other.SomeDataService') + extra

    class Eng(object):
        dry_run = False
        printer = _Printer()
        config_dict = configobj.ConfigObj(text.splitlines())
    return Eng()


def _feed(monkeypatch, answers):
    answers = list(answers)
    prompts = []

    def fake_input(msg):
        prompts.append(msg)
        if not answers:
            raise EOFError
        return answers.pop(0)
    monkeypatch.setattr('builtins.input', fake_input)
    return prompts


def test_installer_puts_service_first_and_keeps_others(monkeypatch):
    _feed(monkeypatch, ['new', ''])
    eng = _inst_engine()
    inst = install.loader()
    assert inst.configure(eng) is True
    assert eng.config_dict['Engine']['Services']['data_services'] == [
        'user.dataingest.DataIngestService', 'user.mqtt.LoopPublisher',
        'user.other.SomeDataService']


def test_installer_prompt_new_with_default_name(monkeypatch):
    prompts = _feed(monkeypatch, ['', ''])
    inst = install.loader()
    inst.configure(_inst_engine())
    assert prompts[0] == ("Do you want to add to existing weewx.sdb database or create a "
                          "new database (existing/new) [new]: ")
    assert prompts[1] == "New database name (without .sdb) [dataingest]: "
    assert inst['config']['DataIngest']['missing_columns'] == 'separate'
    assert inst['config']['Databases']['dataingest_db']['database_name'] == 'dataingest.sdb'


def test_installer_prompt_new_reprompts_until_valid(monkeypatch, capsys):
    prompts = _feed(monkeypatch, ['maybe', 'NEW', 'bad name!', 'weewx', 'airdata.sdb'])
    inst = install.loader()
    inst.configure(_inst_engine())
    assert len(prompts) == 5
    out = capsys.readouterr().out
    assert 'existing or new' in out and "letters, digits" in out and 'main archive' in out
    # A typed .sdb is not doubled up.
    assert inst['config']['Databases']['dataingest_db']['database_name'] == 'airdata.sdb'


def test_installer_prompt_existing(monkeypatch):
    prompts = _feed(monkeypatch, ['existing'])
    inst = install.loader()
    eng = _inst_engine()
    inst.configure(eng)
    assert len(prompts) == 1                      # no name question
    assert inst['config']['DataIngest']['missing_columns'] == 'add'
    assert 'DataBindings' not in inst['config'] and 'Databases' not in inst['config']


def test_installer_no_input_uses_defaults(monkeypatch):
    _feed(monkeypatch, [])
    inst = install.loader()
    eng = _inst_engine()
    inst.configure(eng)
    assert inst['config']['Databases']['dataingest_db']['database_name'] == 'dataingest.sdb'
    assert any('No answer given' in line for line in eng.printer.lines)


def test_installer_command_line_answers(monkeypatch):
    prompts = _feed(monkeypatch, [])
    inst = install.loader()
    inst.process_args(['--database=new', '--database-name', 'my-air'])
    inst.configure(_inst_engine())
    assert prompts == []
    assert inst['config']['Databases']['dataingest_db']['database_name'] == 'my-air.sdb'

    inst = install.loader()
    inst.process_args(['--database-name=solo'])       # implies new
    inst.configure(_inst_engine())
    assert inst['config']['Databases']['dataingest_db']['database_name'] == 'solo.sdb'

    inst = install.loader()
    inst.process_args(['--database=existing', '--unrelated-option'])
    inst.configure(_inst_engine())
    assert inst['config']['DataIngest']['missing_columns'] == 'add'

    with pytest.raises(ValueError):
        install.loader().process_args(['--database-name=../evil'])
    inst = install.loader()
    inst.process_args(['--database-name=weewx'])
    with pytest.raises(ValueError):
        inst.configure(_inst_engine())


def test_installer_reinstall_overwrites_previous_answers(monkeypatch):
    _feed(monkeypatch, ['new', 'second'])
    eng = _inst_engine("""
[DataIngest]
    missing_columns = add
""".replace('\n', '\n'))
    eng.config_dict['Databases']['dataingest_db'] = {'database_name': 'first.sdb',
                                                    'database_type': 'SQLite'}
    install.loader().configure(eng)
    assert eng.config_dict['DataIngest']['missing_columns'] == 'separate'
    assert eng.config_dict['Databases']['dataingest_db']['database_name'] == 'second.sdb'


def test_installer_mysql_main_database(monkeypatch):
    _feed(monkeypatch, ['new', 'airdata'])
    eng = _inst_engine()
    eng.config_dict['DataBindings']['wx_binding']['database'] = 'archive_mysql'
    inst = install.loader()
    inst.configure(eng)
    db = inst['config']['Databases']['dataingest_db']
    assert db['database_name'] == 'airdata' and db['database_type'] == 'MySQL'


def test_installer_dry_run_changes_nothing(monkeypatch):
    _feed(monkeypatch, ['existing'])
    eng = _inst_engine()
    eng.dry_run = True
    before = eng.config_dict.dict()
    assert install.loader().configure(eng) is False
    assert eng.config_dict.dict() == before


# ---------------------------------------------------------------- URL sources

import http.server  # noqa: E402
import threading  # noqa: E402


class _FeedServer(object):
    """Tiny local HTTP server: routes[path] = (status, content_type, body[, headers])."""

    def __init__(self):
        self.routes = {}
        self.requests = []
        self.bodies = []                 # (method, path, body) for every request
        outer = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                self.do_GET()

            def do_GET(self):
                length = int(self.headers.get('Content-Length') or 0)
                self.body = self.rfile.read(length).decode('utf-8') if length else ''
                outer.bodies.append((self.command, self.path, self.body))
                outer.requests.append((self.path, dict(self.headers)))
                route = outer.routes.get(self.path.split('?')[0])
                if route is None:
                    self.send_response(404)
                    self.end_headers()
                    return
                status, ctype, body = route[:3]
                extra = route[3] if len(route) > 3 else {}
                if callable(body):
                    status, body = body(self)
                if isinstance(body, str):
                    body = body.encode('utf-8')
                self.send_response(status)
                self.send_header('Content-Type', ctype)
                for k, v in extra.items():
                    self.send_header(k, v)
                self.end_headers()
                if status != 304:
                    self.wfile.write(body)

            def log_message(self, *a):
                pass

        self.httpd = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.base = 'http://127.0.0.1:%d' % self.httpd.server_address[1]
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


@pytest.fixture
def feed():
    server = _FeedServer()
    yield server
    server.close()


def test_redact_url():
    url = 'https://api.x.com/v1?lat=51.5&appid=SECRET1&apiKey=S2&token=S3&units=metric'
    out = da.redact_url(url)
    assert 'SECRET1' not in out and 'S2' not in out and 'S3' not in out
    assert 'lat=51.5' in out and 'units=metric' in out
    assert 'pw' not in da.redact_url('https://user:pw@host/x')


def test_placeholders_and_resolve():
    cfg = {'Station': {'latitude': '51.5', 'longitude': '-0.12',
                       'altitude': ['100', 'foot']}}
    ph = da.station_placeholders(cfg)
    assert ph == {'latitude': 51.5, 'longitude': -0.12, 'altitude': 30.5}
    assert da.resolve_url('http://h/?lat={latitude}&lon={longitude}', ph) == \
        'http://h/?lat=51.5&lon=-0.12'
    with pytest.raises(ValueError):
        da.resolve_url('http://h/?k={apikey}', ph)


def test_only_http_schemes(tmp_path):
    cfg = make_config(tmp_path, """
[DataIngest]
    [[sources]]
        [[[f]]]
            path = file:///etc/passwd
            mapping = a:b
        [[[ftp]]]
            path = ftp://example.com/x.json
            mapping = a:b
""")
    assert [s.enable for s in da.load_sources(cfg)] == [False, False]


def test_fetch_once_headers_charset_and_save(tmp_path, feed):
    feed.routes['/aq'] = (200, 'application/json; charset=utf-8',
                          '{"current": {"ozone": 41.2, "label": "Ozoné"}}')
    save = tmp_path / 'copy.json'
    s = da.SourceConfig('aq', feed.base + '/aq?appid=XYZ', {'ozone': 'o3'},
                        json_path='current', headers={'X-API-Key': 'k123'},
                        save_to=str(save))
    reader = da.make_reader(s, {})
    reader.fetcher.fetch_once()
    assert reader.values() == {'o3': 41.2}
    path, headers = feed.requests[-1]
    headers = {k.lower(): v for k, v in headers.items()}   # names are case-insensitive
    assert path == '/aq?appid=XYZ'
    assert headers['x-api-key'] == 'k123'
    assert headers['user-agent'].startswith('weewx-data-ingest/')
    assert json.loads(save.read_text())['current']['label'] == 'Ozoné'


def test_fetch_errors_keep_last_good_data(feed):
    feed.routes['/v'] = (200, 'application/json', '{"v": 1}')
    s = da.SourceConfig('v', feed.base + '/v', {'v': 'extraTemp1'})
    reader = da.make_reader(s, {})
    reader.fetcher.fetch_once()
    assert reader.values() == {'extraTemp1': 1.0}

    feed.routes['/v'] = (500, 'text/plain', 'boom')
    with pytest.raises(Exception):
        reader.fetcher.fetch_once()
    assert reader.values() == {'extraTemp1': 1.0}          # still the last good one

    feed.routes['/v'] = (200, 'application/json', '{"v": ')    # garbled: rejected
    with pytest.raises(ValueError):
        reader.fetcher.fetch_once()
    assert reader.values() == {'extraTemp1': 1.0}

    feed.routes['/v'] = (200, 'application/json', '{"v": 5}')
    reader.fetcher.fetch_once()
    assert reader.values() == {'extraTemp1': 5.0}


def test_url_max_age_counts_from_last_good_fetch(feed):
    feed.routes['/v'] = (200, 'application/json', '{"v": 1}')
    s = da.SourceConfig('v', feed.base + '/v', {'v': 'extraTemp1'}, max_age=60)
    reader = da.make_reader(s, {})
    reader.fetcher.fetch_once()
    assert reader.values(now=time.time() + 30) == {'extraTemp1': 1.0}
    assert reader.values(now=time.time() + 120) == {}


def test_oversized_response_rejected(feed, monkeypatch):
    monkeypatch.setattr(da, 'MAX_RESPONSE_BYTES', 100)
    feed.routes['/big'] = (200, 'application/json', '{"v": "%s"}' % ('x' * 500))
    reader = da.make_reader(da.SourceConfig('b', feed.base + '/big', {'v': 'x'}), {})
    with pytest.raises(ValueError):
        reader.fetcher.fetch_once()


def test_no_fetch_yet_injects_nothing(feed):
    reader = da.make_reader(da.SourceConfig('n', feed.base + '/none', {'v': 'x'}), {})
    assert reader.read() == (None, None)


def test_service_fetches_in_background_and_injects(tmp_path, feed):
    feed.routes['/xml'] = (200, 'application/xml',
                           '<?xml version="1.0" encoding="UTF-8"?>'
                           '<r><site status="amber"/><a><v>12</v></a><a><v>48.6</v></a></r>')
    cfg = make_config(tmp_path, """
[Station]
    latitude = 54.0
    longitude = -2.8
[DataIngest]
    [[sources]]
        [[[aurora]]]
            path = %s/xml?lat={latitude}&lon={longitude}
            poll_interval = 10
            json_path = r
            [[[[mapping]]]]
                a.-1.v = auroraActivity
                site.@status = auroraStatus
            [[[[text_values]]]]
                amber = 2
""" % feed.base)
    svc, engine = make_service(cfg)
    fetcher = svc.readers[0].fetcher
    assert fetcher.url.endswith('/xml?lat=54.0&lon=-2.8')
    deadline = time.time() + 5
    while fetcher.latest()[0] is None and time.time() < deadline:
        time.sleep(0.05)
    p = {'dateTime': time.time(), 'usUnits': weewx.US}
    svc.new_loop_packet(Event(packet=p))
    assert p['auroraActivity'] == 48.6 and p['auroraStatus'] == 2.0
    svc.shutDown()
    assert not fetcher._thread.is_alive()


def test_service_unresolvable_url_disables_source(tmp_path):
    cfg = make_config(tmp_path, """
[DataIngest]
    [[sources]]
        [[[bad]]]
            path = https://example.com/?key={api_key}
            mapping = a:extraTemp1
""")
    svc, _ = make_service(cfg)
    assert svc.readers == []


def test_cli_with_url(tmp_path, feed, capsys):
    feed.routes['/aq'] = (200, 'application/json', '{"current": {"ozone": 41.2}}')
    cfg = make_config(tmp_path, """
[DataIngest]
    [[sources]]
        [[[aq]]]
            path = %s/aq?appid=TOPSECRET
            json_path = current
            mapping = ozone:o3
""" % feed.base)
    conf = _write_conf(tmp_path, cfg)
    assert da.main(['--config', conf, 'test']) == 0
    out = capsys.readouterr().out
    assert '41.2' in out and 'TOPSECRET' not in out and 'fetched' in out
    assert da.main(['--config', conf, 'paths', 'aq']) == 0
    assert 'ozone' in capsys.readouterr().out
    assert da.main(['--config', conf, 'paths', feed.base + '/aq']) == 0
    assert 'current.ozone' in capsys.readouterr().out


# ------------------------------------------------------- URL request limits

def _url_src(feed, path='/v', **kw):
    kw.setdefault('poll_interval', 900)
    return da.SourceConfig(kw.pop('name', 'v'), feed.base + path, {'v': 'extraTemp1'}, **kw)


def _count(feed, path='/v'):
    return sum(1 for p, _h in feed.requests if p.split('?')[0] == path)


def test_poll_interval_enforced(feed, tmp_path):
    feed.routes['/v'] = (200, 'application/json', '{"v": 1}')
    f = da.make_reader(_url_src(feed), {}, str(tmp_path)).fetcher
    f.fetch_once(force=False)
    with pytest.raises(da.RateLimited) as e:
        f.fetch_once(force=False)
    assert 890 < e.value.wait <= 900
    assert _count(feed) == 1


def test_limits_and_data_survive_restart(feed, tmp_path):
    feed.routes['/v'] = (200, 'application/json', '{"v": 3}')
    da.make_reader(_url_src(feed), {}, str(tmp_path)).fetcher.fetch_once()
    # "Restart": a brand-new fetcher for the same URL.
    reader = da.make_reader(_url_src(feed), {}, str(tmp_path))
    assert reader.values() == {'extraTemp1': 3.0}       # saved response, no request
    assert reader.fetcher.wait_time()[0] > 800
    with pytest.raises(da.RateLimited):
        reader.fetcher.fetch_once(force=False)
    assert _count(feed) == 1


def test_failures_back_off_never_faster_than_poll(feed, tmp_path):
    feed.routes['/v'] = (500, 'text/plain', 'boom')
    f = da.make_reader(_url_src(feed, poll_interval=600), {}, str(tmp_path)).fetcher
    waits = []
    for _ in range(4):
        with pytest.raises(ValueError):
            f.fetch_once()
        waits.append(f._interval())
    assert waits == [600, 1200, 2400, 3600]             # doubles, capped at 1 h
    feed.routes['/v'] = (200, 'application/json', '{"v": 1}')
    f.fetch_once()
    assert f._interval() == 600 and f.state['failures'] == 0


def test_retry_interval_option(feed, tmp_path):
    feed.routes['/v'] = (500, 'text/plain', 'boom')
    f = da.make_reader(_url_src(feed, poll_interval=3600, retry_interval=120), {},
                       str(tmp_path)).fetcher
    with pytest.raises(ValueError):
        f.fetch_once()
    assert f._interval() == 120


def test_retry_after_is_honoured_even_when_forced(feed, tmp_path):
    feed.routes['/v'] = (429, 'text/plain', 'slow down', {'Retry-After': '1800'})
    f = da.make_reader(_url_src(feed, poll_interval=60), {}, str(tmp_path)).fetcher
    with pytest.raises(ValueError) as e:
        f.fetch_once()
    assert 'Retry-After 1800' in str(e.value)
    with pytest.raises(da.RateLimited) as e:
        f.fetch_once(force=True)
    assert 1790 < e.value.wait <= 1800 and 'Retry-After' in str(e.value)
    assert _count(feed) == 1


def test_parse_retry_after():
    import email.utils
    assert da.parse_retry_after('120') == 120
    when = email.utils.formatdate(time.time() + 600, usegmt=True)
    assert 590 < da.parse_retry_after(when) <= 600
    assert da.parse_retry_after('soon') is None and da.parse_retry_after(None) is None


def test_daily_cap_is_hard(feed, tmp_path):
    feed.routes['/v'] = (200, 'application/json', '{"v": 1}')
    f = da.make_reader(_url_src(feed, max_requests_per_day=2), {}, str(tmp_path)).fetcher
    f.fetch_once()
    f.fetch_once()                                       # forced: interval skipped
    with pytest.raises(da.RateLimited) as e:
        f.fetch_once(force=True)
    assert 'max_requests_per_day = 2' in str(e.value)
    assert _count(feed) == 2
    # Requests older than 24 h no longer count.
    f.state['requests'] = [t - 86401 for t in f.state['requests']]
    f._save_state()
    f.fetch_once()
    assert _count(feed) == 3


def test_conditional_request_304(feed, tmp_path):
    def handler(req):
        if req.headers.get('If-None-Match') == '"abc"':
            return 304, ''
        return 200, '{"v": 7}'
    feed.routes['/v'] = (200, 'application/json', handler, {'ETag': '"abc"'})
    reader = da.make_reader(_url_src(feed), {}, str(tmp_path))
    reader.fetcher.fetch_once()
    first = reader.fetcher.fetched_at
    time.sleep(0.01)
    reader.fetcher.fetch_once()                          # 304 Not Modified
    assert feed.requests[-1][1].get('If-None-Match') == '"abc"'
    assert reader.fetcher.fetched_at > first             # data confirmed current
    assert reader.values() == {'extraTemp1': 7.0}


def test_gzip_response(feed, tmp_path):
    import gzip
    feed.routes['/v'] = (200, 'application/json', gzip.compress(b'{"v": 9}'),
                         {'Content-Encoding': 'gzip'})
    reader = da.make_reader(_url_src(feed), {}, str(tmp_path))
    reader.fetcher.fetch_once()
    assert reader.values() == {'extraTemp1': 9.0}
    assert 'gzip' in feed.requests[-1][1].get('Accept-Encoding', '')


def test_same_url_shared_with_strictest_limits(feed, tmp_path):
    feed.routes['/v'] = (200, 'application/json', '{"v": 1, "w": 2}')
    fetchers = {}
    a = da.make_reader(_url_src(feed, name='a', poll_interval=600), {}, str(tmp_path),
                       fetchers)
    b = da.SourceConfig('b', feed.base + '/v', {'w': 'extraTemp2'}, poll_interval=1800,
                        max_requests_per_day=10)
    b = da.make_reader(b, {}, str(tmp_path), fetchers)
    assert a.fetcher is b.fetcher and len(fetchers) == 1
    assert a.fetcher.poll_interval == 1800 and a.fetcher.max_per_day == 10
    a.fetcher.fetch_once()
    assert a.values() == {'extraTemp1': 1.0} and b.values() == {'extraTemp2': 2.0}
    assert _count(feed) == 1


def test_url_defaults(tmp_path):
    cfg = make_config(tmp_path, """
[DataIngest]
    [[sources]]
        [[[fast]]]
            path = https://example.com/a
            poll_interval = 5
            retry_interval = 1
            mapping = a:extraTemp1
        [[[plain]]]
            path = https://example.com/b
            mapping = a:extraTemp1
        [[[nolimit]]]
            path = https://example.com/c
            max_age = 0
            mapping = a:extraTemp1
        [[[file]]]
            path = /tmp/x.json
            mapping = a:extraTemp1
""")
    s = {x.name: x for x in da.load_sources(cfg)}
    assert s['fast'].poll_interval == 60 and s['fast'].retry_interval == 60
    assert s['plain'].poll_interval == 900 and s['plain'].max_age == 2700
    assert s['nolimit'].max_age == 0
    assert s['file'].max_age == 0


def test_cache_dir_defaults(tmp_path):
    cfg = make_config(tmp_path, "")
    assert da.cache_dir_for(cfg) == os.path.join(str(tmp_path / 'db'), 'dataingest_cache')
    cfg['DataIngest'] = {'cache_dir': 'mycache'}
    assert da.cache_dir_for(cfg) == os.path.join(str(tmp_path), 'mycache')


def test_service_restart_does_not_refetch(tmp_path, feed):
    feed.routes['/v'] = (200, 'application/json', '{"v": 4}')
    cfg = make_config(tmp_path, """
[DataIngest]
    [[sources]]
        [[[v]]]
            path = %s/v
            mapping = v:extraTemp1
""" % feed.base)
    for _ in range(3):                                   # three WeeWX starts
        svc, _ = make_service(cfg)
        fetcher = svc.readers[0].fetcher
        deadline = time.time() + 5
        while fetcher.latest()[0] is None and time.time() < deadline:
            time.sleep(0.05)
        time.sleep(0.2)
        p = {'dateTime': time.time(), 'usUnits': weewx.US}
        svc.new_loop_packet(Event(packet=p))
        assert p['extraTemp1'] == 4.0
        svc.shutDown()
    assert _count(feed) == 1


def test_cli_does_not_poll_every_run(tmp_path, feed, capsys):
    feed.routes['/v'] = (200, 'application/json', '{"v": 5}')
    cfg = make_config(tmp_path, """
[DataIngest]
    [[sources]]
        [[[v]]]
            path = %s/v
            max_requests_per_day = 2
            mapping = v:extraTemp1
""" % feed.base)
    conf = _write_conf(tmp_path, cfg)
    da.main(['--config', conf, 'test'])
    assert 'fetched now' in capsys.readouterr().out
    da.main(['--config', conf, 'test'])
    da.main(['--config', conf, 'paths', 'v'])
    out = capsys.readouterr().out
    assert out.count('using saved response') == 2 and '5.0' in out
    assert _count(feed) == 1
    da.main(['--config', conf, 'test', '--fetch'])       # forced: allowed (2nd of 2)
    da.main(['--config', conf, 'test', '--fetch'])       # cap reached
    out = capsys.readouterr().out
    assert 'fetched now' in out and 'max_requests_per_day = 2 reached' in out
    assert _count(feed) == 2


# ------------------------------------------- tokens, params and auth apart from the URL

def _src_from_conf(tmp_path, text):
    cfg = make_config(tmp_path, "[DataIngest]\n    [[sources]]\n" + text)
    return {s.name: s for s in da.load_sources(cfg)}


def _headers(feed, i=-1):
    return {k.lower(): v for k, v in feed.requests[i][1].items()}


def test_params_kept_apart_from_url(feed, tmp_path):
    feed.routes['/v'] = (200, 'application/json', '{"v": 1}')
    s = _src_from_conf(tmp_path, """
        [[[p]]]
            path = %s/v?units=metric
            mapping = v:extraTemp1
            [[[[params]]]]
                lat = {latitude}
                appid = S3CRET&odd=chars
""" % feed.base)['p']
    reader = da.make_reader(s, {'latitude': 51.5}, str(tmp_path))
    assert 'S3CRET' not in da.redact_url(reader.fetcher.url)
    reader.fetcher.fetch_once()
    path = feed.requests[-1][0]
    assert path.startswith('/v?units=metric&lat=51.5&appid=S3CRET%26odd%3Dchars')
    assert reader.values() == {'extraTemp1': 1.0}


def test_secrets_from_env_and_file(tmp_path, monkeypatch):
    monkeypatch.setenv('DI_TEST_KEY', 'from-env-123')
    keyfile = tmp_path / 'key.txt'
    keyfile.write_text('from-file-456\n')
    s = _src_from_conf(tmp_path, """
        [[[a]]]
            path = https://example.com/x
            mapping = v:extraTemp1
            [[[[headers]]]]
                X-API-Key = env:DI_TEST_KEY
            [[[[params]]]]
                token = file:%s
""" % keyfile)['a']
    assert s.enable
    assert s.headers['X-API-Key'] == 'from-env-123' and s.params['token'] == 'from-file-456'


def test_missing_secret_disables_source_without_leaking(tmp_path, caplog):
    s = _src_from_conf(tmp_path, """
        [[[a]]]
            path = https://example.com/x
            mapping = v:extraTemp1
            [[[[headers]]]]
                X-API-Key = env:DI_NOT_SET_ANYWHERE
        [[[b]]]
            path = https://example.com/y
            mapping = v:extraTemp1
            [[[[auth]]]]
                type = oauth2
                client_id = abc
""")
    assert not s['a'].enable and not s['b'].enable
    assert 'DI_NOT_SET_ANYWHERE is not set' in caplog.text
    assert 'needs token_url, client_secret' in caplog.text


def test_post_form_and_json_body(feed, tmp_path):
    feed.routes['/q'] = (200, 'application/json', '{"v": 2}')
    srcs = _src_from_conf(tmp_path, """
        [[[form]]]
            path = %s/q
            mapping = v:extraTemp1
            [[[[form]]]]
                station = {latitude}
                key = FORMKEY
        [[[json]]]
            path = %s/q
            body = '{"lat": {latitude}, "fields": ["v"]}'
            mapping = v:extraTemp1
        [[[get]]]
            path = %s/q
            method = GET
            mapping = v:extraTemp1
""" % (feed.base, feed.base, feed.base))
    for name in ('form', 'json', 'get'):
        da.make_reader(srcs[name], {'latitude': 51.5}, str(tmp_path)).fetcher.fetch_once()
    (m1, _, b1), (m2, _, b2), (m3, _, b3) = feed.bodies[-3:]
    assert (m1, b1) == ('POST', 'station=51.5&key=FORMKEY')
    assert m2 == 'POST' and json.loads(b2) == {'lat': 51.5, 'fields': ['v']}
    assert _headers(feed, -2)['content-type'] == 'application/json'
    assert (m3, b3) == ('GET', '')


def test_basic_and_bearer_auth(feed, tmp_path):
    import base64
    feed.routes['/v'] = (200, 'application/json', '{"v": 1}')
    srcs = _src_from_conf(tmp_path, """
        [[[basic]]]
            path = %(b)s/v
            mapping = v:extraTemp1
            [[[[auth]]]]
                type = basic
                username = me
                password = pa55word
        [[[bearer]]]
            path = %(b)s/v?x=1
            mapping = v:extraTemp1
            [[[[auth]]]]
                type = bearer
                token = TOK123
        [[[asparam]]]
            path = %(b)s/v?x=2
            mapping = v:extraTemp1
            [[[[auth]]]]
                type = bearer
                token = TOK456
                param = api_key
""" % {'b': feed.base})
    for name in ('basic', 'bearer', 'asparam'):
        da.make_reader(srcs[name], {}, str(tmp_path)).fetcher.fetch_once()
    assert _headers(feed, -3)['authorization'] == \
        'Basic ' + base64.b64encode(b'me:pa55word').decode()
    assert _headers(feed, -2)['authorization'] == 'Bearer TOK123'
    assert feed.requests[-1][0] == '/v?x=2&api_key=TOK456'
    assert 'authorization' not in _headers(feed, -1)


class _TokenServer(object):
    """Adds /token (OAuth2 client credentials) and /data (needs the token)."""

    def __init__(self, feed, lifetime=3600):
        self.issued = []
        self.valid = set()

        def token(req):
            n = len(self.issued) + 1
            tok = 'ACCESS-%d' % n
            self.issued.append((req.headers.get('Authorization'), req.body))
            self.valid.add(tok)
            return 200, json.dumps({'access_token': tok, 'expires_in': lifetime,
                                    'token_type': 'Bearer'})

        def data(req):
            auth = req.headers.get('Authorization', '')
            if auth.replace('Bearer ', '') not in self.valid:
                return 401, '{"error": "unauthorised"}'
            return 200, '{"v": 11}'
        feed.routes['/token'] = (200, 'application/json', token)
        feed.routes['/data'] = (200, 'application/json', data)


def _oauth_src(tmp_path, feed, extra=''):
    return _src_from_conf(tmp_path, """
        [[[o]]]
            path = %(b)s/data
            mapping = v:extraTemp1
            [[[[auth]]]]
                type = oauth2
                token_url = %(b)s/token
                client_id = my-client
                client_secret = my-SECRET-value
                scope = read
                %(extra)s
""" % {'b': feed.base, 'extra': extra})['o']


def test_oauth2_client_credentials_token_reused(feed, tmp_path):
    import base64
    ts = _TokenServer(feed)
    reader = da.make_reader(_oauth_src(tmp_path, feed), {}, str(tmp_path))
    reader.fetcher.fetch_once()
    reader.fetcher.fetch_once()
    assert reader.values() == {'extraTemp1': 11.0}
    assert len(ts.issued) == 1                           # one token, two data requests
    auth, body = ts.issued[0]
    assert auth == 'Basic ' + base64.b64encode(b'my-client:my-SECRET-value').decode()
    assert 'grant_type=client_credentials' in body and 'scope=read' in body
    assert 'my-SECRET-value' not in body                 # sent in the header, not the body
    # Kept across a restart, in an owner-only file.
    again = da.make_reader(_oauth_src(tmp_path, feed), {}, str(tmp_path))
    again.fetcher.fetch_once()
    assert len(ts.issued) == 1
    assert os.stat(again.fetcher.state_path).st_mode & 0o777 == 0o600


def test_oauth2_client_auth_in_body(feed, tmp_path):
    ts = _TokenServer(feed)
    reader = da.make_reader(_oauth_src(tmp_path, feed, 'client_auth = body'), {},
                            str(tmp_path))
    reader.fetcher.fetch_once()
    auth, body = ts.issued[0]
    assert auth is None and 'client_secret=my-SECRET-value' in body


def test_expired_or_revoked_token_is_renewed(feed, tmp_path):
    ts = _TokenServer(feed, lifetime=30)                 # under a minute: renewed each time
    reader = da.make_reader(_oauth_src(tmp_path, feed), {}, str(tmp_path))
    reader.fetcher.fetch_once()
    reader.fetcher.fetch_once()
    assert len(ts.issued) == 2

    ts2 = _TokenServer(feed)                             # server "restarts": old tokens revoked
    reader = da.make_reader(_oauth_src(tmp_path, feed), {}, str(tmp_path))
    reader.fetcher.state['token'], reader.fetcher.state['token_expires'] = \
        'REVOKED', time.time() + 3600
    reader.fetcher._save_state()                         # as a previous run left it
    with pytest.raises(ValueError):
        reader.fetcher.fetch_once()                      # 401
    assert reader.fetcher.state['token'] is None
    reader.fetcher.fetch_once()                          # new token, then data
    assert len(ts2.issued) == 1 and reader.values() == {'extraTemp1': 11.0}


def test_custom_token_endpoint(feed, tmp_path):
    def login(req):
        body = json.loads(req.body)
        if body != {'user': 'me', 'pass': 'pw-123456'}:
            return 403, '{}'
        return 200, '{"data": {"jwt": "JWT-XYZ"}}'

    def data(req):
        ok = req.headers.get('X-Auth') == 'JWT-XYZ'
        return (200, '{"v": 3}') if ok else (401, '{}')
    feed.routes['/login'] = (200, 'application/json', login)
    feed.routes['/d'] = (200, 'application/json', data)
    s = _src_from_conf(tmp_path, """
        [[[c]]]
            path = %(b)s/d
            mapping = v:extraTemp1
            [[[[auth]]]]
                type = token
                token_url = %(b)s/login
                body = '{"user": "me", "pass": "pw-123456"}'
                token_path = data.jwt
                token_lifetime = 600
                header = X-Auth
                prefix = none
""" % {'b': feed.base})['c']
    reader = da.make_reader(s, {}, str(tmp_path))
    reader.fetcher.fetch_once()
    assert reader.values() == {'extraTemp1': 3.0}
    assert 590 < reader.fetcher.state['token_expires'] - time.time() <= 600


def test_secrets_never_logged(feed, tmp_path, caplog):
    import logging
    caplog.set_level(logging.DEBUG)
    feed.routes['/token'] = (500, 'text/plain', 'down')
    s = _oauth_src(tmp_path, feed)
    reader = da.make_reader(s, {}, str(tmp_path))
    with pytest.raises(ValueError) as e:
        reader.fetcher.fetch_once()
    msg = da.scrub(e.value, reader.fetcher.spec.secrets)
    assert 'token request: HTTP 500' in msg
    assert da.scrub('id my-client secret my-SECRET-value', reader.fetcher.spec.secrets) \
        == 'id *** secret ***'
    assert 'my-SECRET-value' not in caplog.text and 'my-client' not in caplog.text


def test_different_auth_not_shared(feed, tmp_path):
    srcs = _src_from_conf(tmp_path, """
        [[[a]]]
            path = %(b)s/v
            mapping = v:extraTemp1
            [[[[headers]]]]
                X-API-Key = KEY-A
        [[[b]]]
            path = %(b)s/v
            mapping = v:extraTemp2
            [[[[headers]]]]
                X-API-Key = KEY-B
""" % {'b': feed.base})
    fetchers = {}
    a = da.make_reader(srcs['a'], {}, str(tmp_path), fetchers)
    b = da.make_reader(srcs['b'], {}, str(tmp_path), fetchers)
    assert a.fetcher is not b.fetcher and a.fetcher.cache_path != b.fetcher.cache_path


# ---------------------------------------- a database column never mixes units

def test_no_group_column_with_declared_units_is_refused(tmp_path):
    src = tmp_path / 'm.json'
    write_json(src, {'a': 5.0, 'b': 7.0, 'c': 20.0})
    cfg = make_config(tmp_path, """
[DataIngest]
    [[sources]]
        [[[metric]]]
            path = %s
            unit_system = METRIC
            mapping = a:mysteryA, c:extraTemp1
        [[[percol]]]
            path = %s
            mapping = b:mysteryB
            [[[[units]]]]
                mysteryB = km_per_hour
        [[[undeclared]]]
            path = %s
            mapping = a:mysteryC
""" % (src, src, src))
    svc, _ = make_service(cfg)                         # database: US
    p = {'dateTime': time.time(), 'usUnits': weewx.US}
    svc.new_loop_packet(Event(packet=p))
    assert 'mysteryA' not in p and 'mysteryB' not in p  # would be metric in a US column
    assert p['extraTemp1'] == pytest.approx(68.0)       # grouped: converted
    assert p['mysteryC'] == 5.0                         # no units declared: as read
    problems = da.conversion_problems(svc.readers[0].source, weewx.US)
    assert 'will NOT be stored' in problems[0]


def test_declared_units_matching_database_are_fine_without_group(tmp_path):
    src = tmp_path / 'm.json'
    write_json(src, {'a': 5.0})
    cfg = make_config(tmp_path, """
[DataIngest]
    [[sources]]
        [[[us]]]
            path = %s
            unit_system = US
            mapping = a:mysteryD
""" % src)
    svc, _ = make_service(cfg)
    p = {'dateTime': time.time(), 'usUnits': weewx.METRIC}
    svc.new_loop_packet(Event(packet=p))
    assert p['mysteryD'] == 5.0


def test_same_unit_in_every_system_needs_no_conversion(tmp_path):
    src = tmp_path / 'aq.json'
    write_json(src, {'pm': 12.5, 'h': 55})
    cfg = make_config(tmp_path, """
[DataIngest]
    [[sources]]
        [[[aq]]]
            path = %s
            unit_system = METRICWX
            mapping = pm:pm2_5, h:outHumidity
""" % src)
    svc, _ = make_service(cfg)
    p = {'dateTime': time.time(), 'usUnits': weewx.US}
    svc.new_loop_packet(Event(packet=p))
    assert p['pm2_5'] == 12.5 and p['outHumidity'] == 55.0


def test_groups_cannot_regroup_an_existing_column(tmp_path, caplog):
    da.register_groups({'groups': {'extraTemp1': 'group_speed',
                                   'outHumidity': 'group_percent',
                                   'brandNewCol': 'group_temperature'}})
    try:
        assert weewx.units.obs_group_dict['extraTemp1'] == 'group_temperature'
        assert weewx.units.obs_group_dict['brandNewCol'] == 'group_temperature'
        assert 'would mix units' in caplog.text and 'outHumidity' not in caplog.text
    finally:
        weewx.units.obs_group_dict.pop('brandNewCol', None)


def test_database_holds_one_unit_per_column(tmp_path):
    """Metric and US files feeding a US database: everything stored in US."""
    c = tmp_path / 'c.json'
    f = tmp_path / 'f.json'
    write_json(c, {'t': 20.0, 'w': 36.0})
    write_json(f, {'t': 50.0, 'w': 10.0})
    cfg = make_config(tmp_path, """
[DataIngest]
    missing_columns = add
    [[sources]]
        [[[metric]]]
            path = %s
            unit_system = METRICWX
            mapping = t:extraTemp1, w:windGust
            [[[[units]]]]
                windGust = km_per_hour
        [[[us]]]
            path = %s
            unit_system = US
            mapping = t:extraTemp2, w:windSpeed
""" % (c, f))
    svc, engine = make_service(cfg)
    start(svc, engine)
    rec = {'dateTime': int(time.time()), 'usUnits': weewx.US, 'interval': 5}
    svc.new_archive_record(Event(record=rec))
    mgr = engine.db_binder.get_manager('wx_binding')
    mgr.addRecord(rec)
    row = mgr.getSql("SELECT usUnits, extraTemp1, windGust, extraTemp2, windSpeed "
                     "FROM archive")
    assert row[0] == weewx.US
    assert row[1] == pytest.approx(68.0) and row[2] == pytest.approx(22.369, rel=1e-3)
    assert row[3] == 50.0 and row[4] == 10.0
