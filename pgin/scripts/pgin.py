import os
import sys
import importlib

import click
from jinja2 import Environment, FileSystemLoader
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
        self.template_dir = os.path.join(Config.PROJECTDIR, 'templates')
        self.template_env = Environment(loader=FileSystemLoader(self.template_dir))
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
    ctx.obj = Migration(home=os.path.abspath(home))
    ctx.obj.verbose = verbose
    for key, value in config:
        ctx.obj.set_config(key, value)
# _____________________________________________


def turn_to_python_package(path):
    with open(os.path.join(path, '__init__.py'), 'w') as fw:
        fw.write('')
# _____________________________________________


@cli.command()
@click.argument('project')
@pass_migration
def init(migration, project):
    """
        Initiates the project DB migrations.
    """

    click.echo('Initiating project %s migrations on path %s' % (project, migration.home))
    create_directory(migration.home)
    turn_to_python_package(migration.home)
    for d in ['deploy', 'revert']:
        create_directory(os.path.join(migration.home, d))
        turn_to_python_package(os.path.join(migration.home, d))

    migration.dba = DBAdmin(conf=conf, dbname=project)
    migration.dba.create_meta_schema()
    migration.dba.create_changes_table()
# _____________________________________________


def create_script(migration, direction, name):
    template_file = '%s.tmpl' % direction
    script_file = '%s.py' % name
    script_path = os.path.join(migration.home, direction, script_file)
    tmpl = migration.template_env.get_template(template_file)
    params = {
        'name': name
    }
    code = tmpl.render(params)
    with open(script_path, 'w') as fw:
        fw.write(code)

    logger.info("Created script: %r", os.path.join(direction, script_file))
# _____________________________________________


@cli.command()
@click.argument('name')
@pass_migration
def add(migration, name):
    """
    Adds migration script to the plan
    """
    for direction in ['deploy', 'revert']:
        create_script(migration, direction, name)
# _____________________________________________


@cli.command()
@pass_migration
def deploy(migration):
    """
    Deploys undeployed
    """
    click.echo("Migration config: %r" % migration.config)

    module_name = 'appschema'
    mod = importlib.import_module('%s.deploy.%s' % (conf['MIGRATIONS_PKG'], module_name))
    mod.deploy(project=migration.config['project'], conn=migration.dba.conn)
# _____________________________________________
