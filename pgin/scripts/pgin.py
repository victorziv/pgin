import os
import sys
import importlib
import click
import jsonlines
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
        self.plan = os.path.join(self.home, 'plan.jsonl')
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

    plan_file = '%s.plan' % migration.project
    plan_path = os.path.join(migration.home, plan_file)
    if os.path.exists(plan_path):
        logger.info("Project %s migration facility already initiated", migration.project)
        return

    logger.info('Initiating project %s migrations', migration.project)
    logger.info('Migration container path: %s', migration.home)
    create_directory(migration.home)
    turn_to_python_package(migration.home)

    for d in ['deploy', 'revert']:
        create_directory(os.path.join(migration.home, d))
        logger.info("Created %s/", d)
        turn_to_python_package(os.path.join(migration.home, d))

    dba = DBAdmin(conf=conf, dbname=migration.project, dbuser=migration.project_user)
    dba.createdb()
    dburi = Config.db_connection_uri(dbname=migration.project, dbuser=migration.project_user)
    dba.conn = dba.connectdb(dburi)
    dba.cursor = dba.conn.cursor()
    dba.create_meta_schema()
    dba.create_changes_table()
    dba.set_search_path()
    dba.cursor.close()
    dba.conn.close()
    create_plan(migration.plan)
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
        fw.write(code.strip())

    logger.info("Created script: %r", os.path.join(direction, script_file))
# _____________________________________________


def create_plan(plan):
    """
    Create emply jsonl file
    """
    with open(plan, 'w') as f:
        f.write('')
# _____________________________________________


def update_plan(migration, change, msg):
    with jsonlines.open(migration.plan, mode='a') as writer:
        writer.write({'change': change, 'msg': msg})
# _____________________________________________


def script_already_exists(migration, direction, script_name):

    script_file = '%s.py' % script_name
    script_path = os.path.join(migration.home, direction, script_file)
    print(script_path)
    if os.path.exists(script_path):
        print("Script {} already exists".format(script_path))
        return True
    return False
# _____________________________________________


def validate_plan_record_not_exists(migration, change):
    with jsonlines.open(migration.plan, mode='r') as reader:
        for l in reader:
            if l['change'] == change:
                print('Change %r already exists in %s' % (change, migration.plan))
                sys.exit(0)

# ============= Commands ==================


@cli.command()
@click.argument('change')
@click.option('-m', '--msg', required=True, help="Short migration description")
@pass_migration
def add(migration, change, msg):
    """
    Adds migration script to the plan
    """

    validate_plan_record_not_exists(migration, change)
    update_plan(migration, change, msg)
    for direction in ['deploy', 'revert']:
        if not script_already_exists(migration, direction, change):
            create_script(migration, direction, change)
# _____________________________________________


@cli.command()
@pass_migration
def deploy(migration):
    """
    Deploys undeployed
    """

    try:
        module_name = 'appschema'
        mod = importlib.import_module('%s.deploy.%s' % (conf['MIGRATIONS_PKG'], module_name))
        deploy_cls = getattr(mod, module_name.capitalize())
        dba = DBAdmin(conf=conf, dbname=migration.project, dbuser=migration.project_user)
        dburi = Config.db_connection_uri(migration.project, migration.project_user)
        logger.info('Deploying changes to: %s', dburi)
        conn = dba.connectdb(dburi)
        deploy = deploy_cls(project=migration.project, project_user=migration.project_user, conn=conn)
        logger.info("+ %s %s ok", module_name, '.' * 30)
        deploy()
    except Exception:
        logger.exception("Migration exception")
    finally:
        conn.close()
# _____________________________________________
