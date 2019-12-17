import os
import sys

import click
from config import Configurator
conf = Configurator.configure()
logger = Configurator.set_logging(name=conf['PROJECT'], console_logging=True)

from lib.helpers import create_directory  # noqa
from dba import DBAdmin  # noqa
dba = DBAdmin(conf=conf)
# =====================================


class Migration(object):

    def __init__(self, home):
        self.logger = logger
        self.conf = conf
        self.project = conf['PROJECT']
        self.home = home
        self.config = {}
        self.verbose = False
    # ___________________________________

    def set_config(self, key, value):
        self.config[key] = value
        if self.verbose:
            click.echo('  config[%s] = %s' % (key, value), file=sys.stderr)
    # ___________________________________

    def __repr__(self):
        return '<Repo %r>' % self.home
# _____________________________________________


pass_migration = click.make_pass_decorator(Migration)
# _____________________________________________


@click.group()
@click.option('--home', envvar='MIGRATION_HOME', default=os.path.join(conf['PROJECTDIR'], 'migration'),
              metavar='PATH', help='Changes the migration default container')
@click.option('--config', nargs=2, multiple=True,
              metavar='KEY VALUE', help='Overrides a config key/value pair.')
@click.option('--verbose', '-v', is_flag=True, help='Enables verbose mode.')
@click.version_option('0.1.0')
@click.pass_context
def cli(ctx, home, config, verbose):
    """
    postmig is a command line tool for HWInfo project DB migrations management
    """
    ctx.obj = Migration(os.path.abspath(home))
    ctx.obj.verbose = verbose
    for key, value in config:
        ctx.obj.set_config(key, value)
# _____________________________________________


@cli.command()
@pass_migration
def init(migration):
    """Initiates the project DB migrations.

    """

    click.echo('Initiating project %s migrations on path %s' % (migration.project, migration.home))
    create_directory(migration.home)
    for d in ['deploy', 'revert']:
        create_directory(os.path.join(migration.home, d))
    dba.createdb(newdb=conf['DBNAME'], newdb_owner=conf['PROJECT_USER'])
    dba.create_meta_schema()
    dba.create_changes_table()
# _____________________________________________


@cli.command()
@pass_migration
def add(migration):
    pass
# _____________________________________________
