import os
import importlib

import click
from jinja2 import Environment, FileSystemLoader
from pgin.config import Configurator, Config
conf = Configurator.configure()
logger = Configurator.set_logging(name=conf['LOGGER_NAME'], console_logging=True)

from pgin.lib.helpers import create_directory  # noqa
from pgin.dba import DBAdmin  # noqa
# =====================================


class Migration(object):

    def __init__(self, home, project, project_user):
        self.logger = logger
        self.conf = conf
        self.home = home
        self.project = project
        self.project_user = project_user
        self.template_dir = os.path.join(Config.PROJECTDIR, 'templates')
        self.template_env = Environment(loader=FileSystemLoader(self.template_dir))
    # ___________________________________

# =============================================


pass_migration = click.make_pass_decorator(Migration)
# _____________________________________________


def validate_migration_home(ctx, param, value):
    if value is None:
        raise click.BadParameter('MIGRATION_HOME env variable has to be set to a valid path')

    return value
# _____________________________________________


def validate_project(ctx, param, value):
    if value is None:
        raise click.BadParameter(
            'PROJECT environment variable has to be set to the parent project name')

    return value
# _____________________________________________


def validate_project_user(ctx, param, value):
    if value is None:
        raise click.BadParameter(
            'PROJECT_USER environment variable has to be set to the parent project generic user name')

    return value
# _____________________________________________


@click.group()
@click.option(
    '--home',
    envvar='MIGRATION_HOME',
    metavar='PATH',
    callback=validate_migration_home,
    help='Sets migration container folder'
)
@click.option(
    '--project',
    envvar='PROJECT',
    callback=validate_project,
    help='Parent project name'
)
@click.option(
    '--project_user',
    envvar='PROJECT_USER',
    callback=validate_project_user,
    help='Parent project generic user account'
)
@click.version_option('0.1.0')
@click.pass_context
def cli(ctx, home, project, project_user):
    """
    postmig is a command line tool for HWInfo project DB migrations management
    """
    ctx.obj = Migration(home=os.path.abspath(home), project=project, project_user=project_user)
# _____________________________________________


def turn_to_python_package(path):
    with open(os.path.join(path, '__init__.py'), 'w') as fw:
        fw.write('')
# _____________________________________________


@cli.command()
@pass_migration
def init(migration):
    """
        Initiates the project DB migrations.
    """

    click.echo('Initiating project %s migrations on path %s' % (migration.project, migration.home))
    create_directory(migration.home)
    turn_to_python_package(migration.home)
    for d in ['deploy', 'revert']:
        create_directory(os.path.join(migration.home, d))
        turn_to_python_package(os.path.join(migration.home, d))

    migration.dba = DBAdmin(
        conf=conf, dbname=migration.project, dbuser=migration.project_user)
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

    module_name = 'appschema'
    mod = importlib.import_module('%s.deploy.%s' % (conf['MIGRATIONS_PKG'], module_name))
    logger.info('Deploying project: %s', migration.project)
    deploy_cls = getattr(mod, module_name.capitalize())
    dba = DBAdmin(conf=conf, dbname=migration.project, dbuser=migration.project_user)
    deploy = deploy_cls(project=migration.project, project_user=migration.project_user, conn=dba.conn)
    try:
        deploy()
    except Exception:
        logger.exception("Migration exception")

# _____________________________________________
