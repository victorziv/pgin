import os
import sys
# import hashlib
import uuid
import importlib
import click
import re
import jsonlines
import psycopg2
import psycopg2.extras
import datetime
from jinja2 import Environment, FileSystemLoader
from tabulate import tabulate
from config import Config, Configurator
# =================================================

runtype = os.getenv('CONFIG_ENV')
if runtype is None:
    print("ERROR: $CONFIG_ENV environment variable is not set")
    sys.exit(1)

conf = Configurator.configure(config_type=runtype)

from lib import applogging  # noqa 
execid = 'pgin'
logger = applogging.set_logger(
    logger_name=execid,
    logpath=os.path.join(conf['LOGDIR'], '%s.log' % execid),
    log_to_console=True
)

from pgin.lib.helpers import create_directory  # noqa
from pgin.dba import DBAdmin  # noqa
MSG_LENGTH = 60
# _____________________________________________


def get_version():
    rootdir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    with open(os.path.join(rootdir, 'VERSION')) as fp:
        version = fp.read()

    return version.strip()
# =====================================


class MutuallyExclusiveOption(click.Option):
    def __init__(self, *args, **kwargs):
        self.mutually_exclusive = set(kwargs.pop('mutually_exclusive', []))
        hlp = kwargs.get('help', '')

        if self.mutually_exclusive:
            ex_str = ', '.join(self.mutually_exclusive)
            kwargs['help'] = '''
                {}
                NOTE: This argument is mutually exclusive with
                arguments: [ {} ]
                '''.format(hlp, ex_str)

        super(MutuallyExclusiveOption, self).__init__(*args, **kwargs)
    # ____________________________

    def handle_parse_result(self, ctx, opts, args):
        if self.mutually_exclusive.intersection(opts) and self.name in opts:
            raise click.UsageError(
                'Illegal usage: `{}` is mutually exclusive with'
                'arguments `{}`'.format(self.name, ', '.join(self.mutually_exclusive)))

        return super(MutuallyExclusiveOption, self).handle_parse_result(ctx, opts, args)
# =========================================================


class Migration(object):

    def __init__(self, project, project_user):
        self.logger = logger
        self.conf = conf
        self.workdir = 'migration'
        self.home = os.path.abspath(os.path.join(conf['ROOTDIR'], project, self.workdir))
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


def deploy_testing(project, dba, dbuser, dbname):
    """
    Deploys all changes into testing DB
    """

    try:
        migration = Migration(project=project, project_user=dbuser)
        dburi = Config.db_connection_uri(dbname=dbname, dbuser=dbuser)
        dba.conn = dba.connectdb(dburi)
        dba.cursor = dba.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        dba.set_search_path(schema=project)
        dba.show_search_path()

        changes = plan_file_entries(migration)

        for line in changes:
            change = line['name']
            deploy = load_deploy_script(migration, dba, change)

            click.echo(message="+ {} {} ".format(change, '.' * (MSG_LENGTH - len(change))), nl=False)
            deploy()
            click.echo(click.style('ok', fg='green'))

    except psycopg2.ProgrammingError as pe:
        click.echo(click.style('fail', fg='red'))
        click.echo("!!! Error in deploy: {}".format(pe))
        logger.exception('Exception in deploy')
    except Exception:
        logger.exception('Exception in deploy')
    finally:
        disconnect_dba(dba)
# _____________________________________________


def change_deployed(migration, change):
    dba = connect_dba(migration)
    changeid = dba.fetch_deployed_changeid_by_name(change)
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

    click.echo("Created script: {}".format(os.path.join(direction, script_file)))
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
    dba.set_search_path(schema=conf['PROJECT'])
    return dba
# _____________________________________________


def create_pgin_metaschema(dba):
    dba.create_meta_schema()
    dba.create_plan_table()
    dba.create_changes_table()
    dba.create_tags_table()
# _____________________________________________


def disconnect_dba(dba):
    dba.cursor.close()
    dba.conn.close()
# _____________________________________________


def change_entry_or_last(migration, name):
    '''
    If passed change is None, the last line index is returned
    '''
    lines = []
    change_ind = None
    with jsonlines.open(migration.plan) as reader:
        for ind, l in enumerate(reader):
            if l['name'] == name:
                change_ind = ind
            lines.append(l)

        if change_ind is None:
            change_ind = -1

    return lines, change_ind
# _____________________________________________


def plan_file_entries(migration):
    '''
    If passed change is None, the last line index is returned
    '''
    lines = []
    with jsonlines.open(migration.plan) as reader:
        for line in reader:
            lines.append(line)
    return lines
# _____________________________________________


def get_change_deploy(migration, dba, name):
#     changeid = get_changeid(change)
    mod = importlib.import_module('%s.deploy.%s' % (migration.workdir, name))
    deploy_cls = getattr(mod, name.capitalize())

    deploy = deploy_cls(
        project=migration.project,
        project_user=migration.project_user,
        conf=migration.conf,
        conn=dba.conn,
        logger=migration.logger
    )

    return deploy
# _____________________________________________


# def get_changeid(change):
#     return hashlib.sha1(change.encode('utf-8')).hexdigest()
# _____________________________________________

def generate_changeid(change):
    return uuid.uuid4().hex
# _____________________________________________


def get_change_revert(migration, dba, change):
    mod = importlib.import_module('%s.revert.%s' % (migration.workdir, change))
    revert_cls = getattr(mod, change.capitalize())

    revert = revert_cls(
        project=migration.project,
        project_user=migration.project_user,
        conf=migration.conf,
        conn=dba.conn,
        logger=migration.logger
    )

    return revert
# _____________________________________________


def load_deploy_script(migration, dba, change):
    mod = importlib.import_module('%s.deploy.%s' % (migration.workdir, change))
    deploy_cls = getattr(mod, change.capitalize())

    deploy = deploy_cls(
        project=migration.project,
        project_user=migration.project_user,
        conf=migration.conf,
        conn=dba.conn,
        logger=migration.logger
    )

    return deploy
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


def remove_from_plan(migration, name):
    lines, change_ind = change_entry_or_last(migration, name)
    lines.pop(change_ind)
    write_plan(migration, lines)
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


def plan_record_exists(dba, migration, change):
    changeid = dba.fetch_planned_changeid_by_name(change)
    if changeid is None:
        return False
    return True
# _____________________________________________


def populate_plan_table(migration, dba):
    click.echo("Sync plan file into DB metaschema plan table")
    changes = plan_file_entries(migration)
    for change in changes:
        changeid = change['changeid']
        name = change['name']

        click.echo(message="+ {} {} ".format(name, '.' * (MSG_LENGTH - len(name))), nl=False)
        dba.apply_planned(changeid, name, change['msg'])
        click.echo(click.style('ok', fg='green'))
# _____________________________________________


def set_tag(migration, tag, msg, name):
    lines = []
    with jsonlines.open(migration.plan) as reader:
        tag_set = False
        for line in reader:
            if line['name'] == name:
                line['tag'] = tag
                line['tagmsg'] = msg
                tag_set = True
            lines.append(line)

        if not tag_set:
            last = lines[-1]
            change = last['name']
            last['tag'] = tag
            last['tagmsg'] = msg

    with jsonlines.open(migration.plan, mode='w') as writer:
        for line in lines:
            writer.write(line)

    dba = connect_dba(migration)
    dba.apply_tag(line['changeid'], tag, msg)

    click.echo("Tag {} applied to change {}".format(tag, change))
# _____________________________________________


def write_plan(migration, lines):
    with jsonlines.open(migration.plan, mode='w') as writer:
        for line in lines:
            writer.write(line)
# _____________________________________________


def validate_migration_home(ctx, param, value):
    if value is None:
        raise click.BadParameter('MIGRATION_HOME env variable has to be set to a valid path')

    return value
# _____________________________________________


def validate_project(ctx, param, value):
    if value is None:
        raise click.BadParameter('PROJECT environment variable or --project command line parameter has to be set')

    return value
# _____________________________________________


def validate_project_user(ctx, param, value):
    if value is None:
        raise click.BadParameter(
            'PROJECT_USER environment variable  or --project_user command line parameter has to be set')

    return value
# _____________________________________________


def update_plan(migration, changeid, name, msg):
    with jsonlines.open(migration.plan, mode='a') as writer:
        writer.write({
            'changeid': changeid,
            'name': name,
            'msg': msg,
        })
# _____________________________________________


def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=datetime.timezone.utc).astimezone(tz=None)

# ============= Commands ==================


@click.group()
@click.option(
    '--project',
    envvar='PROJECT',
    callback=validate_project,
    help='Parent project name. Default: PROJECT env variable value'
)
@click.option(
    '--project_user',
    envvar='PROJECT_USER',
    callback=validate_project_user,
    help='Parent project generic user account. Default: PROJECT_USER env variable value'
)
@click.version_option(get_version())
@click.pass_context
def cli(ctx, project, project_user):
    """
    pgin is a command line tool for PostgreSQL DB migrations management.
    Run with Python 3.6+.
    Uses psycopg2 DB driver.
    """
    ctx.obj = Migration(project=project, project_user=project_user)
# _____________________________________________


@cli.command()
@click.argument('change')
@click.option('-m', '--msg', required=True, help="Short migration description")
@pass_migration
def add(migration, name, msg):
    """
    Adds migration script to the plan
    """

    os.chdir(migration.home)
    dba = connect_dba(migration)
    if plan_record_exists(dba, migration, name):
        click.echo(message='Change {} already exists in migration plan'.format(name))
        sys.exit(0)

    changeid = generate_changeid()
    update_plan(migration, changeid, name, msg)
    dba.apply_planned(changeid, name, msg)

    for direction in ['deploy', 'revert']:
        if not script_exists(migration, direction, name):
            create_script(migration, direction, name)

    click.echo("Change '{}' has been added".format(name))
# _____________________________________________


@cli.command()
@click.option('--to')
@pass_migration
def deploy(migration, to=None):
    """
    Deploys pending changes
    """

    try:
        dba = connect_dba(migration)
        to, msg = figure_deploy_to_change(dba, migration, to)

        click.echo(msg)

        changes = plan_file_entries(migration)

        for line in changes:
            changeid = line['changeid']
            name = line['name']
            deploy = get_change_deploy(migration, dba, name)

            if change_deployed(migration, changeid):
                continue

            click.echo(message="+ {} {} ".format(name, '.' * (MSG_LENGTH - len(name))), nl=False)
            deploy()
            dba.apply_change(changeid, name)
            if 'tag' in line:
                dba.apply_tag(changeid, line['tag'], line['tagmsg'])

            click.echo(click.style('ok', fg='green'))
            if name == to:
                break

    except psycopg2.ProgrammingError as pe:
        click.echo(click.style('fail', fg='red'))
        click.echo("!!! Error in deploy: {}".format(pe))
        logger.exception('Exception in deploy')
    except Exception:
        logger.exception('Exception in deploy')
    finally:
        disconnect_dba(dba)
# _____________________________________________


@cli.command()
@click.option('--newdb', is_flag=True, required=False, help="If set to TRUE drops and re-creates existent DB")
@pass_migration
def init(migration, newdb=False):
    """
        Initiates the project DB migrations.
    """

    click.echo("Initiating project '{}' migrations".format(migration.project))
    click.echo('Migration container path: {}'.format(migration.home))

    create_directory(migration.home)
    turn_to_python_package(migration.home)

    if not os.path.exists(migration.plan):
        logger.debug("Creating migration plan file: %r", migration.plan)
        create_plan(migration.plan)

    for d in ['deploy', 'revert']:
        create_directory(os.path.join(migration.home, d))
        click.echo("Created {}/".format(d))
        turn_to_python_package(os.path.join(migration.home, d))

    try:
        dba = DBAdmin(conf=conf, dbname=migration.project, dbuser=migration.project_user)
        dba.revoke_connect_from_db()

        if newdb:
            sure = input("Sure to drop existing DB {}? (Yes/No) ".format(migration.project).lower())
            if sure in ['y', 'yes']:
                click.echo("Dropping DB {}".format(migration.project))
                dba.dropdb()
            else:
                click.echo("DB {} will not be dropped".format(migration.project))

        click.echo("Creating DB {} if not already exists".format(migration.project))
        dba.createdb()
        dba.grant_connect_to_db()
        dba = connect_dba(migration)
        create_pgin_metaschema(dba)
        populate_plan_table(migration, dba)
    finally:
        disconnect_dba(dba)
# _____________________________________________


@cli.command()
@click.option('-y', '--yes', is_flag=True, callback=not_remove_if_false, expose_value=False, prompt='Remove change?')
@click.argument('change', required=True)
@pass_migration
def remove(migration, change):
    """
    Adds migration script to the plan
    """

    try:
        dba = connect_dba(migration)
        os.chdir(migration.home)
        if not plan_record_exists(dba, migration, change):
            click.echo("Change {} not found in migration plan".format(change))
            sys.exit(0)

        if change_deployed(migration, change):
            click.echo("Cannot remove a deployed change {}. Revert first".format(change))
            sys.exit(1)

        click.echo("Removing change %s from migration plan" % change)
        remove_from_plan(migration, change)
        dba.remove_change_from_plan(change)
    finally:
        disconnect_dba(dba)

    for direction in ['deploy', 'revert']:
        remove_script(migration, direction, change)
# _____________________________________________


def figure_deploy_to_change(dba, migration, to):

    if to is None:
        msg = "Deploying all pending changes to '{}'".format(migration.project)
        return to, msg

    # Check 'to' is a tag
    name = dba.fetch_change_by_tag(to)
    if name:
        msg = "Deploying pending changes from '{}'. Last tag to deploy: '{}'".format(
            migration.project, to)
        return name, msg

    # 'to' is supposed to be a change name
    if plan_record_exists(dba, migration, to):
        msg = "Deploying pending changes from '{}'. Last change to deploy: '{}'".format(
            migration.project, to)
        return to, msg

    click.echo(message="Change '{}' not found".format(to))
    sys.exit(1)
# _____________________________________________


def figure_revert_upto_change(dba, migration, upto):
    logger.debug("Revert upto: %r", upto)
    pat1 = re.compile(r'^HEAD$')
    pat2 = re.compile(r'^HEAD~(\d+)$')

    if upto is None:
        msg = "Reverting all deployed changes from '{}'".format(migration.project)
        return upto, msg

    # check upto is tag
    name = dba.fetch_change_by_tag(upto)
    if name:
        msg = "Reverting deployed changes from '{}'. Last tag to revert: '{}'".format(
            migration.project, upto)
        return name, msg

    # Figure out HEAD[~\d+] pattern passed
    if pat1.match(upto):
        # last change
        change = dba.fetch_deployed_changes(limit=1)[0]
        name = change['name']
        msg = "Reverting deployed changes from '{}'. Last change to revert: '{}'".format(
            migration.project, name)
        return name, msg

    match2 = pat2.match(upto)
    if match2:
        changes_back = match2.group(1)

        # change with offset <changes_back>
        try:
            change = dba.fetch_deployed_changes(offset=int(changes_back), limit=1)[0]
            name = change['name']
            msg = "Reverting deployed changes from '{}'. Last change to revert: '{}'".format(
                migration.project, name)
        except IndexError:
            msg = "Reverting all deployed changes from '{}'".format(migration.project)
            name = None

        return name, msg

    if plan_record_exists(dba, migration, upto):
        msg = "Reverting deployed changes from '{}'. Last change to revert: '{}'".format(
            migration.project, upto)
        return upto, msg

    click.echo(message="Change '{}' not found".format(upto))
    sys.exit(0)
# _____________________________________________


@cli.command()
@click.option('-y', '--yes', is_flag=True, callback=not_revert_if_false, expose_value=False, prompt='Revert?')
@pass_migration
@click.option('--to')
def revert(migration, to=None):
    """
    Revert deployed
    """
    try:

        dba = connect_dba(migration)
        to, msg = figure_revert_upto_change(dba, migration, to)

        click.echo(msg)

        changes = dba.fetch_deployed_changes()

        for change_d in changes:
            name = change_d['name']
            changeid = change_d['changeid']
            click.echo(message="- %s %s " % (name, '.' * (MSG_LENGTH - len(name))), nl=False)
            revert = get_change_revert(migration, dba, name)
            revert()
            dba.remove_change(changeid)
            click.echo(click.style('ok', fg='green'))
            if name == to:
                break

    except Exception:
        click.echo(click.style('fail', fg='red'))
        logger.exception("Exception in revert")
    finally:
        disconnect_dba(dba)
# _____________________________________________


@cli.command()
@pass_migration
def status(migration):
    """
    Report deployment status
    """

    try:
        dba = connect_dba(migration)
        click.echo("# On database: {}".format(migration.project))

        last_deployed_change = dba.fetch_last_deployed_change()
        if last_deployed_change:
            click.echo("# Last Change ID: {}".format(last_deployed_change['changeid']))
            click.echo("# Last Change Name: {}".format(last_deployed_change['name']))
            dt = utc_to_local(last_deployed_change['applied']).strftime('%Y-%m-%d %H:%M:%S')
            click.echo("# Applied: {}".format(dt))
            click.echo('')

        lines = plan_file_entries(migration)
        undeployed = []
        for line in lines:
            if not change_deployed(migration, line['name']):
                undeployed.append(line)

        if len(undeployed) == len(lines):
            click.echo("No changes deployed")
            sys.exit(0)

        if len(undeployed):
            tablist = [(c['name'], c['msg'], c.get('tag'), c.get('tagmsg')) for c in undeployed]
            click.echo("Undeployed changes:")
            click.echo("")
            click.echo(tabulate(tablist, headers=['Change', 'Message', 'Tag', 'Tag Message'], floatfmt=".1f"))
        else:
            click.echo("Nothing to deploy (up-to-date)")
    finally:
        disconnect_dba(dba)
# _____________________________________________


@cli.command()
@pass_migration
def sync(migration):
    """
    Sync plan file with DB metaschema plan table
    """

    try:
        dba = connect_dba(migration)
        create_pgin_metaschema(dba)
        populate_plan_table(migration, dba)
    finally:
        disconnect_dba(dba)
# _____________________________________________


@cli.group()
def tag():
    """
    pgin tag commands: add, remove or list
    """
    pass
# _____________________________________________


@tag.command('add')
@click.option('-t', '--tag', required=True)
@click.option('-m', '--msg', required=True, help="The new tag message")
@click.option('-c', '--change', help="""Change name to attach tag to""")
@pass_migration
def tag_add(migration, tag, msg, change=None):
    """
    Apply tag to a change.
    If no change passed, the tag is applied to the last change
    If another tag has been found attached to the change,
    it is replaced.
    """
    os.chdir(migration.home)
    lines, change_ind = change_entry_or_last(migration, change)
    change_line = lines[change_ind]

    if 'tag' in change_line:
        click.echo(
            click.style(
                'Tag {} already applied to change {}'.format(change_line['tag'], change_line['name']),
                fg='yellow'
            )
        )
        sure = input("Sure to replace it with tag {}? (Yes/No) ".format(tag)).lower()
        if sure != 'y' and sure != 'yes':
            click.echo("Tag was not replaced")
            sys.exit(0)

    change_line['tag'] = tag
    change_line['tagmsg'] = msg

    write_plan(migration, lines)

    dba = connect_dba(migration)
    dba.apply_tag(change_line['changeid'], tag, msg)

    click.echo(click.style("Tag '{}' was applied to change '{}'".format(tag, change_line['name']), fg='green'))
# _____________________________________________


@tag.command('list')
@pass_migration
def tag_list(migration):
    """
    Apply tag to a change.
    If no change passed, the tag is applied to the last change
    """

    dba = connect_dba(migration)
    tags = dba.fetch_tags()
    tag_list = [(t['name'], t['tag'], t['tagmsg']) for t in tags]
    click.echo(tabulate(tag_list, headers=['Change', 'Tag', 'Message'], floatfmt=".1f"))
# _____________________________________________


@tag.command('remove')
@click.option('-y', '--yes', is_flag=True, callback=not_remove_if_false, expose_value=False, prompt='Remove tag?')
@click.option('-t', '--tag', required=True)
@pass_migration
def tag_remove(migration, tag):
    """
    Remove a tag.
    """
    os.chdir(migration.home)
    dba = connect_dba(migration)
    tag_change = dba.fetch_change_by_tag(tag)
    if not tag_change:
        click.echo(click.style("No change with tag '{}' was found".format(tag), fg='yellow'))
        sys.exit(0)

    click.echo("Removing tag '{}' applied to change {}".format(tag, tag_change))

    lines, change_ind = change_entry_or_last(migration, tag_change)
    change_line = lines[change_ind]
    del change_line['tag']
    del change_line['tagmsg']

    write_plan(migration, lines)

    dba.remove_tag(tag_change)
    click.echo(click.style("Tag '{}' was removed".format(tag), fg='green'))
# _____________________________________________
