#
#    weewx-data-ingest installer
#
#    Copyright (C) 2025-2026 Ian Millard
#    GNU General Public License v3 -- see LICENSE.
#
"""Install with:  weectl extension install weewx-data-ingest.zip

The installer asks where new columns should go:

    Do you want to add to existing weewx.sdb database or create a new
    database (existing/new) [new]:
    New database name (without .sdb) [dataingest]:

To install without prompts, pass the answers on the command line:

    weectl extension install weewx-data-ingest.zip --database=existing
    weectl extension install weewx-data-ingest.zip --database=new --database-name=airdata
"""

import argparse
import re
from io import StringIO

import configobj
from weecfg.extension import ExtensionInstaller

VERSION = "1.0.0"

SERVICE = 'user.dataingest.DataIngestService'
BINDING = 'dataingest_binding'
DATABASE = 'dataingest_db'       # fixed key, so uninstall finds it whatever the name
DEFAULT_NAME = 'dataingest'
NAME_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_-]*$')

# weectl merges this without overwriting anything already in weewx.conf.
# configure() fills in missing_columns and the database from the answers.
# Example sources live in README.md -- a disabled example here would just
# clutter every install.
CONFIG = """
[DataIngest]
    # Inject values from external JSON / XML / text files into loop packets
    # and archive records. Source options are documented in README.md.
    enable = true

    # What to do with a mapped column that is not in the main archive:
    #   separate - store it in a separate database (dataingest_binding),
    #              created and extended automatically. Main archive untouched.
    #   add      - add a REAL column to the main archive at startup.
    #   warn     - log a warning only; value is in loop packets, not archived.
    # Set by the installer from your answer (new -> separate, existing -> add).
    missing_columns = separate

    # Unit groups for new columns, so reports can convert and label them.
    [[groups]]
        # o3 = group_concentration

    [[sources]]
        # [[[example]]]
        #     path = /var/www/html/jsondata/example.json
        #     json_path = current
        #     max_age = 900
        #     [[[[mapping]]]]
        #         json_key = db_column

[DataBindings]
    # Separate database for columns that are not in the main archive.
    # Use it in reports with  $day(data_binding='dataingest_binding').o3.avg
    [[dataingest_binding]]
        database = dataingest_db
        table_name = archive
        manager = weewx.manager.DaySummaryManager
        schema = user.dataingest.schema

[Databases]
    [[dataingest_db]]
        database_name = dataingest.sdb
        database_type = SQLite
"""


def loader():
    return DataIngestInstaller()


def main_database(config_dict):
    """(database_name, database_type) of the main archive (wx_binding)."""
    try:
        key = config_dict['DataBindings']['wx_binding']['database']
        db = config_dict['Databases'][key]
        return db.get('database_name', 'weewx.sdb'), db.get('database_type', 'SQLite')
    except (KeyError, TypeError):
        return 'weewx.sdb', 'SQLite'


def clean_name(raw):
    """User input -> bare database name, or None if unusable. A typed
    '.sdb' is tolerated and removed: the suffix is added by the code."""
    name = (raw or '').strip()
    if name.lower().endswith('.sdb'):
        name = name[:-4]
    return name if NAME_RE.match(name) else None


def database_file(name, database_type):
    """The database_name to write: '<name>.sdb' for SQLite, bare for MySQL."""
    return name + '.sdb' if database_type.lower() == 'sqlite' else name


class DataIngestInstaller(ExtensionInstaller):
    def __init__(self):
        super(DataIngestInstaller, self).__init__(
            version=VERSION,
            name='dataingest',
            description='Inject data from external JSON, XML or text files into WeeWX '
                        'loop packets and archive records, adding database columns '
                        'or a separate database as needed.',
            author='Ian Millard',
            author_email='weatherboyian@gmail.com',
            data_services=SERVICE,
            config=configobj.ConfigObj(StringIO(CONFIG)),
            files=[('bin/user', ['bin/user/dataingest.py'])],
        )
        self.answer_choice = None
        self.answer_name = None

    # -- answers -----------------------------------------------------------

    def process_args(self, args):
        """Extra 'weectl extension install' arguments, for unattended installs."""
        parser = argparse.ArgumentParser(prog='weewx-data-ingest installer', add_help=False)
        parser.add_argument('--database', choices=('existing', 'new'))
        parser.add_argument('--database-name')
        ns, _unknown = parser.parse_known_args(args or [])
        self.answer_choice = ns.database
        if ns.database_name is not None:
            self.answer_name = clean_name(ns.database_name)
            if self.answer_name is None:
                raise ValueError("--database-name '%s': use letters, digits, '_' or '-'"
                                 % ns.database_name)
            if self.answer_choice is None:
                self.answer_choice = 'new'

    def _ask(self, engine, main_name):
        """Return ('existing'|'new', name or None). Answers given on the
        command line are used as-is; otherwise prompt. With no input at all
        (end of file -- e.g. run from a script) the defaults are used."""
        choice, name = self.answer_choice, self.answer_name
        main_stem = main_name[:-4] if main_name.lower().endswith('.sdb') else main_name
        try:
            if choice is None:
                choice = self._prompt(
                    "Do you want to add to existing %s database or create a new database "
                    "(existing/new)" % main_name, 'new', ('existing', 'new'))
            if choice == 'new' and name is None:
                while True:
                    raw = self._prompt("New database name (without .sdb)", DEFAULT_NAME)
                    name = clean_name(raw)
                    if name is None:
                        print("  Use letters, digits, '_' or '-' (e.g. airdata).")
                    elif name == main_stem:
                        print("  That is the main archive. Choose another name.")
                    else:
                        break
        except EOFError:
            print()
            choice = choice or 'new'
            if choice == 'new' and name is None:
                name = DEFAULT_NAME
            engine.printer.out("No answer given: using '%s'%s. Pass --database=existing|new "
                               "[--database-name=NAME] to choose."
                               % (choice, " named '%s'" % name if choice == 'new' else ''),
                               level=1)
        if choice == 'new' and name == main_stem:
            raise ValueError("The new database cannot have the same name as the main "
                             "archive (%s)" % main_name)
        return choice, name

    @staticmethod
    def _prompt(prompt, default, options=None):
        msg = "%s [%s]: " % (prompt, default)
        while True:
            value = input(msg).strip().lower() if options else input(msg).strip()
            if not value:
                return default
            if not options or value in options:
                return value
            print("  Please answer %s." % ' or '.join(options))

    # -- configuration -----------------------------------------------------

    def configure(self, engine):
        """Apply the database answers, and put DataIngestService first in
        data_services so any service that uses loop packets (MQTT, live-data
        or similar) sees the injected values. Nothing else in the service
        lists is changed."""
        cfg = engine.config_dict
        dry_run = getattr(engine, 'dry_run', False)
        main_name, main_type = main_database(cfg)
        choice, name = self._ask(engine, main_name)
        changed = False

        # The template is merged only where weewx.conf has no value yet, so
        # on a re-install the answers are also written straight into cfg.
        existing_ingest = cfg.get('DataIngest')
        if choice == 'existing':
            self['config']['DataIngest']['missing_columns'] = 'add'
            # No separate database needed.
            for section, key in (('DataBindings', BINDING), ('Databases', DATABASE)):
                self['config'][section].pop(key, None)
                if not self['config'][section]:
                    self['config'].pop(section)
            engine.printer.out("New columns will be added to the existing %s database."
                               % main_name, level=1)
            if isinstance(existing_ingest, dict) and not dry_run:
                existing_ingest['missing_columns'] = 'add'
                changed = True
        else:
            db_file = database_file(name, main_type)
            self['config']['DataIngest']['missing_columns'] = 'separate'
            self['config']['Databases'][DATABASE]['database_name'] = db_file
            self['config']['Databases'][DATABASE]['database_type'] = main_type
            engine.printer.out("New columns will go in a new %s database '%s' (binding "
                               "'%s')." % (main_type, db_file, BINDING), level=1)
            if not dry_run:
                if isinstance(existing_ingest, dict):
                    existing_ingest['missing_columns'] = 'separate'
                    changed = True
                db_section = (cfg.get('Databases') or {}).get(DATABASE)
                if isinstance(db_section, dict):
                    db_section['database_name'] = db_file
                    db_section['database_type'] = main_type
                    changed = True

        services = cfg.setdefault('Engine', {}).setdefault('Services', {})
        data = services.get('data_services', [])
        if isinstance(data, str):
            data = [s.strip() for s in data.split(',') if s.strip()]
        data = list(data)
        new = [SERVICE] + [s for s in data if s != SERVICE]
        if new != data and not dry_run:
            services['data_services'] = new
            changed = True
        return changed
