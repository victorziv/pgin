import os
import sys

import click
from jinja2 import Template
from config import Configurator, Config
conf = Configurator.configure()
logger = Configurator.set_logging(name=conf['LOGGER_NAME'], console_logging=True)

from lib.helpers import create_directory  # noqa
from dba import DBAdmin  # noqa
# =====================================


class Migration(object):

    def __init__(self, home):
        self.logger = logger
        self.conf = conf
        self.home = home
        self.config = {}
        self.verbose = False
        self.templates_path = os.path.join(Config.PROJECTDIR, 'templates')
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
@click.argument('project')
@pass_migration
def init(migration, project):
    """
        Initiates the project DB migrations.
    """

    click.echo('Initiating project %s migrations on path %s' % (project, migration.home))
    migration.project = project
    create_directory(migration.home)
    for d in ['deploy', 'revert']:
        create_directory(os.path.join(migration.home, d))
    dba = DBAdmin(conf=conf, dbname=project)
    dba.create_meta_schema()
    dba.create_changes_table()
# _____________________________________________


def create_script(migration, path):
    with open(path, 'w') as fw:
        fw.write('go daddy\n')
# _____________________________________________


@cli.command()
@click.argument('name')
@pass_migration
def add(migration, name):
    print("Templates path: %r" % migration.templates_path)
    for d in ['deploy', 'revert']:
        create_script(migration, os.path.join(migration.home, d, '%s.py' % name))
# _____________________________________________
