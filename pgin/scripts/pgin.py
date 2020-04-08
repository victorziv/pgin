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
logger = applogging.set_logger(logger_name=conf['PROJECT'], log_to_console=True)

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
        self.plan_name = 'plan.jsonl'
        self.plan = os.path.join(self.home, self.plan_name)
        self.project = project
        self.project_user = project_user
        pgindir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
        self.template_dir = os.path.join(pgindir, 'templates')
        self.template_env = Environment(loader=FileSystemLoader(self.template_dir))
    # ___________________________________

# =============================================


pass_migration = click.make_pass_decorator(Migration)
# _____________________________________________


def change_deployed(migration, change):
    dba = connect_dba(migration)
    changeid = dba.fetch_deployed_changeid_by_name(change)
    if changeid is not None:
        return True
    return False
# _____________________________________________


def change_is_planned(migration, change):
    dba = connect_dba(migration)
    changeid = dba.fetch_planned_changeid_by_name(change)
    if changeid is not None:
        return True
    return False
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
    click.echo("Creating migration plan: {}".format(plan))
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


def create_pgin_metaschema(dba):
    dba.create_meta_schema()
    dba.create_plan_table()
    dba.create_changes_table()
    dba.create_tags_table()
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


def not_remove_if_false(ctx, param, value):
    if not value:
        click.echo("Nothing removed")
        ctx.exit()
# _____________________________________________


def remove_from_meta_plan(migration, change):
    dba = connect_dba(migration)
    dba.remove_change_from_plan(change)
# _____________________________________________


def remove_from_plan(migration, change):
    try:
        fpr = open(migration.plan)
        reader = jsonlines.Reader(fpr)

        tmp_plan = "tmp-%s" % migration.plan_name
        tmp_plan_path = os.path.join(os.path.dirname(migration.plan), tmp_plan)
        fpw = open(tmp_plan_path, 'w')
        writer = jsonlines.Writer(fpw)

        for l in reader:
            if l['change'] == change:
                continue
            writer.write(l)

    finally:
        fpr.close()
        fpw.close()
        os.rename(tmp_plan_path, migration.plan)
# _____________________________________________


def remove_script(migration, direction, change):
    os.chdir(migration.home)
    script_file = '%s.py' % change
    script_path = '%s/%s' % (direction, script_file)
    if os.path.exists(script_path):
        click.echo("Removing script {}".format(script_path))
        os.remove(script_path)
# _____________________________________________


def script_exists(migration, direction, script_name):

    os.chdir(migration.home)
    script_file = '%s.py' % script_name
    script_path = '%s/%s' % (direction, script_file)
    if os.path.exists(script_path):
        click.echo("Script {} exists".format(script_path))
        return True
    return False
# _____________________________________________


def turn_to_python_package(path):
    with open(os.path.join(path, '__init__.py'), 'w') as fw:
        fw.write('')
# _____________________________________________


def plan_record_exists(migration, change):
    if change_is_planned(migration, change):
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


def update_plan(migration, change, msg):
    with jsonlines.open(migration.plan, mode='a') as writer:
        writer.write({
            'change': change,
            'msg': msg,
        })

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
    dba = connect_dba(migration)
    changeid = get_changeid(change)
    dba.apply_planned(changeid, change, msg)

    for direction in ['deploy', 'revert']:
        if not script_exists(migration, direction, change):
            create_script(migration, direction, change)
# _____________________________________________


@cli.command()
@click.argument('upto', required=False)
@pass_migration
def deploy(migration, upto=None):
    """
    Deploys pending changes
    """

    if upto is None:
        msg = 'Deploying all pending changes to %s' % migration.project
    else:

        if not change_is_planned(migration, upto):
            click.echo("Change {} is not found in migration plan".format(upto))
            sys.exit(1)

        msg = 'Deploying pending changes to %s. Last change to deploy: %s' % (migration.project, upto)

    click.echo(msg)

    try:
        dba = connect_dba(migration)

        with jsonlines.open(migration.plan, mode='r') as reader:

            for l in reader:
                change = l['change']
                deploy, changeid = get_change_deploy(migration, dba, change)

                if change_deployed(migration, change):
                    continue

                click.echo(message="+ %s %s " % (change, '.' * (MSG_LENGTH - len(change))), nl=False)
                deploy()
                dba.apply_change(changeid, change)
                if 'tag' in l:
                    dba.apply_tag(changeid, l['tag'], l['tagmsg'])

                click.echo(click.style('ok', fg='green'))
                if change == upto:
                    break

    except psycopg2.ProgrammingError as pe:
        click.echo(click.style('fail', fg='red'))
        click.echo("!!! Error in deploy: {}".format(pe))
    except Exception as e:
        click.echo(click.style('fail', fg='red'))
        logger.error("Exception in deploy: %s", e)
    finally:
        disconnect_dba(dba)
# _____________________________________________


@cli.command()
@click.option('-f', '--force', is_flag=True, required=False, help="Forcibly re-initiate DB and migration installment")
@pass_migration
def init(migration, force=False):
    """
        Initiates the project DB migrations.
    """

    logger.debug("Migration plan path: %r", migration.plan)
    if os.path.exists(migration.plan) and not force:
        logger.info("Project %s migration facility already initiated", migration.project)
        sys.exit(0)

    logger.info('Initiating project %s migrations', migration.project)
    logger.info('Migration container path: %s', migration.home)
    create_directory(migration.home)
    turn_to_python_package(migration.home)

    for d in ['deploy', 'revert']:
        create_directory(os.path.join(migration.home, d))
        logger.info("Created %s/", d)
        turn_to_python_package(os.path.join(migration.home, d))

    try:
        dba = DBAdmin(conf=conf, dbname=migration.project, dbuser=migration.project_user)
        dba.resetdb()
        dba = connect_dba(migration)
        create_pgin_metaschema(dba)
    finally:
        dba.cursor.close()
        dba.conn.close()

    if os.path.exists(migration.plan) and not force:
        click.echo("Migration plan {} already exists".format(migration.plan))
        sys.exit(0)

    create_plan(migration.plan)
# _____________________________________________


@cli.command()
@click.option('-y', '--yes', is_flag=True, callback=not_remove_if_false, expose_value=False, prompt='Remove change?')
@click.argument('change', required=True)
@pass_migration
def remove(migration, change):
    """
    Adds migration script to the plan
    """

    os.chdir(migration.home)
    if not plan_record_exists(migration, change):
        click.echo("Change {} not found in migration plan".format(change))
        sys.exit(0)

    if change_deployed(migration, change):
        click.echo("Cannot remove a deployed change {}. Revert first".format(change))
        sys.exit(1)

    click.echo("Removing change %s from migration plan" % change)
    remove_from_plan(migration, change)
    remove_from_meta_plan(migration, change)

    for direction in ['deploy', 'revert']:
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
        msg = 'Reverting all deployed changes from %s' % migration.project
    else:
        msg = 'Reverting deployed changes from %s. Last change to revert: %s' % (migration.project, downto)

    click.echo(msg)

    try:
        dba = connect_dba(migration)

        changes = dba.fetch_deployed_changes()

        for change_d in changes:
            change = change_d['name']
            changeid = change_d['changeid']
            click.echo(message="- %s %s " % (change, '.' * (MSG_LENGTH - len(change))), nl=False)
            revert = get_change_revert(migration, dba, change)
            revert()
            dba.remove_change(changeid)
            click.echo(click.style('ok', fg='green'))
            if change == downto:
                break

    except Exception:
        click.echo(click.style('fail', fg='red'))
        logger.exception("Exception in revert")
    finally:
        disconnect_dba(dba)
# _____________________________________________


def set_tag(migration, tag, msg, change):
    lines = []
    with jsonlines.open(migration.plan) as reader:
        tag_set = False
        for l in reader:
            if l['change'] == change:
                l['tag'] = tag
                l['tagmsg'] = msg
                tag_set = True
            lines.append(l)

        if not tag_set:
            last = lines[-1]
            change = last['change']
            last['tag'] = tag
            last['tagmsg'] = msg

    with jsonlines.open(migration.plan, mode='w') as writer:
        for l in lines:
            writer.write(l)

    changeid = get_changeid(change)
    dba = connect_dba(migration)
    dba.apply_tag(changeid, tag, msg)

    click.echo("Tag {} applied to change {}".format(tag, change))
# _____________________________________________


@cli.command()
@click.argument('tag', required=True)
@click.option('-c', '--change', help="""
    Change name to attach tag to. Tag is attached to the last change if not provided""")
@click.option('-m', '--msg', required=True, help="The new tag message")
@pass_migration
def tag(migration, msg, tag, change=None):
    """
    Apply tag to a change
    """
    os.chdir(migration.home)
    set_tag(
        migration=migration,
        tag=tag,
        msg=msg,
        change=change
    )
# _____________________________________________
