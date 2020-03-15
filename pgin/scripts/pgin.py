import os
import sys
import hashlib
import importlib
import click
import jsonlines
import psycopg2
from jinja2 import Environment, FileSystemLoader

from config import Config, Configurator
runtype = os.getenv('%s_CONFIG' % Config.PROJECT.upper())
if runtype is None:
    print("ERROR: $%s_CONFIG env. variable is not set" % Config.PROJECT.upper())
    sys.exit(1)

conf = Configurator.configure(config_type=runtype)

from lib import applogging  # noqa 
logger = applogging.set_frontend_logger(conf['PROJECT'])

from pgin.lib.helpers import create_directory  # noqa
from pgin.dba import DBAdmin  # noqa
MSG_LENGTH = 40
# _____________________________________________


def get_version():
    rootdir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    with open(os.path.join(rootdir, 'VERSION')) as fp:
        version = fp.read()

    return version.strip()
# =====================================


class Migration(object):

    def __init__(self, home, project, project_user):
        self.logger = logger
        self.conf = conf
        self.home = home
        self.plan = os.path.join(self.home, 'plan.jsonl')
        self.project = project
        self.project_user = project_user
        pgindir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
        self.template_dir = os.path.join(pgindir, 'templates')
        self.template_env = Environment(loader=FileSystemLoader(self.template_dir))
    # ___________________________________

# =============================================


pass_migration = click.make_pass_decorator(Migration)
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
        fw.write("%s\n" % code)

    logger.info("Created script: %r", os.path.join(direction, script_file))
# _____________________________________________


def create_plan(plan):
    """
    Create emply jsonl file
    """
    with open(plan, 'w') as f:
        f.write('')
# _____________________________________________


def connect_dba(migration):
    dba = DBAdmin(conf=conf, dbname=migration.project, dbuser=migration.project_user)
    dburi = Config.db_connection_uri(migration.project, migration.project_user)
    dba.conn = dba.connectdb(dburi)
    dba.cursor = dba.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    return dba
# _____________________________________________


def disconnect_dba(dba):
    dba.conn.close()
# _____________________________________________


def get_change_deploy(migration, dba, change):
    changeid = get_changeid(change)
    mod = importlib.import_module('%s.deploy.%s' % (conf['DBMIGRATION_PKG'], change))
    deploy_cls = getattr(mod, change.capitalize())

    deploy = deploy_cls(
        project=migration.project,
        project_user=migration.project_user,
        conf=migration.conf,
        conn=dba.conn
    )

    return deploy, changeid
# _____________________________________________


def get_changeid(change):
    return hashlib.sha1(change.encode('utf-8')).hexdigest()
# _____________________________________________


def get_change_revert(migration, dba, change):
    mod = importlib.import_module('%s.revert.%s' % (conf['DBMIGRATION_PKG'], change))
    revert_cls = getattr(mod, change.capitalize())

    revert = revert_cls(
        project=migration.project,
        project_user=migration.project_user,
        conf=migration.conf,
        conn=dba.conn
    )

    return revert
# _____________________________________________


def not_revert_if_false(ctx, param, value):
    if not value:
        click.echo("Nothing reverted")
        ctx.exit()
# _____________________________________________


def script_exists(migration, direction, script_name):

    os.chdir(migration.home)
    script_file = '%s.py' % script_name
    script_path = '%s/%s' % (direction, script_file)
    if os.path.exists(script_path):
        print("Script {} already exists".format(script_path))
        return True
    return False
# _____________________________________________


def turn_to_python_package(path):
    with open(os.path.join(path, '__init__.py'), 'w') as fw:
        fw.write('')
# _____________________________________________


def update_plan(migration, change, msg):
    with jsonlines.open(migration.plan, mode='a') as writer:
        writer.write({
            'change': change,
            'msg': msg,
        })
# _____________________________________________


def plan_record_exists(migration, change):
    with jsonlines.open(migration.plan, mode='r') as reader:
        for l in reader:
            if l['change'] == change:
                return True

    return False
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


# ============= Commands ==================


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
@click.version_option(get_version())
@click.pass_context
def cli(ctx, home, project, project_user):
    """
    postmig is a command line tool for HWInfo project DB migrations management
    """
    ctx.obj = Migration(home=os.path.abspath(home), project=project, project_user=project_user)
# _____________________________________________


@cli.command()
@click.argument('change')
@click.option('-m', '--msg', required=True, help="Short migration description")
@pass_migration
def add(migration, change, msg):
    """
    Adds migration script to the plan
    """

    os.chdir(migration.home)
    if plan_record_exists(migration, change):
        click.echo(message='Change {} already exists in migration plan'.format(change))
        sys.exit(0)

    update_plan(migration, change, msg)
    for direction in ['deploy', 'revert']:
        if not script_exists(migration, direction, change):
            create_script(migration, direction, change)
# _____________________________________________


@cli.command()
@click.argument('upto', required=False)
@pass_migration
def deploy(migration, upto=None):
    """
    Deploys undeployed
    """

    if upto is None:
        msg = 'Deploying all changes to %s' % migration.project
    else:
        msg = 'Deploying changes to %s. Last change to deploy: %s' % (migration.project, upto)

    click.echo(msg)

    try:
        dba = connect_dba(migration)

        with jsonlines.open(migration.plan, mode='r') as reader:

            for l in reader:
                change = l['change']
                click.echo(message="+ %s %s " % (change, '.' * (MSG_LENGTH - len(change))), nl=False)
                deploy, changeid = get_change_deploy(migration, dba, change)
                deploy()
                dba.apply_change(changeid, change)

                click.echo(click.style('ok', fg='green'))
                if change == upto:
                    break

    except Exception as e:
        click.echo(click.style('fail', fg='red'))
        logger.error("Exception in deploy: %s", e)
        revert(migration)
    finally:
        disconnect_dba(dba)
# _____________________________________________


@cli.command()
@pass_migration
def init(migration):
    """
        Initiates the project DB migrations.
    """

    plan_path = migration.plan
    logger.debug("Migration plan path: %r", plan_path)
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
    dba.cursor.close()
    dba.conn.close()
    create_plan(migration.plan)
# _____________________________________________


@cli.command()
@click.argument('change', required=True)
@pass_migration
def remove(migration, change):
    """
    Adds migration script to the plan
    """

    os.chdir(migration.home)
    validate_plan_record_not_exists(migration, change)
    remove_from_plan(migration, change)
    for direction in ['deploy', 'revert']:
        if script_exists(migration, direction, change):
            remove_script(migration, direction, change)
# _____________________________________________


@cli.command()
@click.option('-y', '--yes', is_flag=True, callback=not_revert_if_false, expose_value=False, prompt='Revert?')
@click.argument('downto', required=False)
@pass_migration
def revert(migration, downto=None):
    """
    Revert deployed
    """
    if downto is None:
        msg = 'Reverting all changes from %s' % migration.project
    else:
        msg = 'Reverting changes from %s. Last change to revert: %s' % (migration.project, downto)

    click.echo(msg)

    try:
        dba = connect_dba(migration)

        changes = dba.fetch_deployed_changes()

        for change_d in changes:
            change = change_d['name']
            changeid = change_d['changeid']
            click.echo(message="- %s %s " % (change, '.' * 30), nl=False)
            revert = get_change_revert(migration, dba, change)
            revert()
            dba.remove_change(changeid)
            click.echo(click.style('ok', fg='green'))
            if change == downto:
                break

    except Exception:
        click.echo(click.style('fail', fg='red'))
        logger.exception("Exception in revert")
        deploy(migration)
    finally:
        disconnect_dba(dba)
# _____________________________________________
