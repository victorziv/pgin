import os
import sys
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
import toml
# =================================================

from pgin.lib.helpers import create_directory  # noqa
from pgin.dba import DBAdmin  # noqa
MSG_LENGTH = 60

# TODO: might be a subject of configuration later on
MIGRATION_DIR = 'dbmigration'
PLAN_FILE = 'plan.json'
DEPLOY_DIR = 'deploy'
REVERT_DIR = 'revert'
CONF_FILE = 'pgin.conf'
CONF_PATH_FILE = '.pgin_confpath'
# /TODO: might be a subject of configuration later on
# _____________________________________________


def create_containers(conf):
    plan = conf['plan']
    home = conf['home']
    if not os.path.exists(plan):
        create_plan(plan)
        click.echo("Created {}".format(plan))

    for d in [DEPLOY_DIR, REVERT_DIR]:
        migration_dir = os.path.join(home, d)
        create_directory(migration_dir)
        click.echo("Created {}/".format(migration_dir))
        turn_to_python_package(migration_dir)

    return conf
# _____________________________________________


def get_version():
    rootdir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    with open(os.path.join(rootdir, 'VERSION')) as fp:
        version = fp.read()

    return version.strip()
# _____________________________________________


def init_config(pgin_project, dbuser, topdir):
    home = os.path.join(topdir, MIGRATION_DIR, pgin_project)

    create_directory(home)
    click.echo("Created {}".format(home))
    turn_to_python_package(home)

    conf = {
        'project': pgin_project,
        'topdir': topdir,
        'home': home,
        'dbuser': dbuser,
        'migration_container': MIGRATION_DIR,
        'plan_file': PLAN_FILE,
        'plan': os.path.join(home, PLAN_FILE)

    }
    conf_file = os.path.join(home, CONF_FILE)
    save_conf_path(conf_file)
    with open(conf_file, 'w') as wf:
        toml.dump(conf, wf)
    os.environ["PGIN_CONF"] = conf_file

    click.echo("Created {}".format(conf_file))
    return create_containers(conf)
# _____________________________________________


def init_db(conf, newdb):

    dbname = conf['project']
    dbuser = conf['dbuser']
    plan = conf['plan']

    try:
        dba = DBAdmin(dbname=dbname, dbuser=dbuser)
        dba.revoke_connect_from_db()

        if newdb:
            sure = input("Sure to drop existing DB {}? (Yes/No) ".format(dbname).lower())
            if sure in ['y', 'yes']:
                upgrade_plan_file(plan)
                click.echo("Dropping DB {}".format(dbname))
                dba.dropdb()
            else:
                click.echo("DB {} will not be dropped".format(dbname))

        click.echo("Creating DB {} if not already exists".format(dbname))
        dba.createdb()
        dba.grant_connect_to_db()
        dba = connect_dba(dbname=dbname, dbuser=dbuser)
        create_pgin_metaschema(dba)
        populate_plan_table(dba, plan)
    finally:
        disconnect_dba(dba)
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

    def __init__(self):
        pgindir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
        self.template_dir = os.path.join(pgindir, 'templates')
        self.template_env = Environment(loader=FileSystemLoader(self.template_dir))
    # ___________________________________

# =============================================


pass_migration = click.make_pass_decorator(Migration)
# _____________________________________________


# def deploy_testing(project, dba, dbuser, dbname):
#     """
#     Deploys all changes into testing DB
#     """

#     try:
#         migration = Migration(project=project, project_user=dbuser)
#         dburi = db_connection_uri(dbname=dbname, dbuser=dbuser)
#         dba.conn = dba.connectdb(dburi)
#         dba.cursor = dba.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
#         dba.set_search_path(schema=project)
#         dba.show_search_path()

#         changes = plan_file_entries(migration)

#         for line in changes:
#             change = line['name']
#             deploy = load_deploy_script(migration, dba, change)

#             click.echo(message="+ {} {} ".format(change, '.' * (MSG_LENGTH - len(change))), nl=False)
#             deploy()
#             click.echo(click.style('ok', fg='green'))

#     except psycopg2.ProgrammingError as pe:
#         click.echo(click.style('fail', fg='red'))
#         click.echo("!!! Error in deploy: {}".format(pe))
#         logger.exception('Exception in deploy')
#     except Exception:
#         logger.exception('Exception in deploy')
#     finally:
#         disconnect_dba(dba)
# _____________________________________________


def change_deployed(migration, changeid):
    dba = connect_dba(migration)
    return dba.fetch_change_deployed(changeid)
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


def connect_dba(dbname, dbuser, dbschema=None):
    dba = DBAdmin(dbname=dbname, dbuser=dbuser)
    dba.conn = dba.connectdb(dba.dburi)
    dba.cursor = dba.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    if dbschema is None:
        dbschema = dbname
    dba.set_search_path(schema=dbschema)
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


def get_change_deploy(migration, dba, name):
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


def generate_changeid():
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


def do_not_if_false(ctx, param, value):
    if not value:
        click.echo("Nothing done")
        ctx.exit()
# _____________________________________________


def plan_file_entries(plan):
    '''
    If passed change is None, the last line index is returned
    '''
    lines = []
    fp = open(plan)
    reader = jsonlines.Reader(fp)
    for line in reader.iter(type=dict, skip_empty=True):
        lines.append(line)
    return lines
# _____________________________________________


def remove_from_plan(migration, name):
    lines, change_ind = change_entry_or_last(migration, name)
    lines.pop(change_ind)
    write_plan(migration, lines)
# _____________________________________________


def rename_in_plan(migration, changeid, old_name, new_name):
    click.echo("Renaming in plan file: {} to {}".format(old_name, new_name))
    lines = plan_file_entries(plan)
    for ln in lines:
        if uuid.UUID(ln['changeid']) == uuid.UUID(changeid):
            ln['name'] = new_name
            break
    write_plan(migration, lines)
# _____________________________________________


def remove_script(migration, direction, name):
    os.chdir(migration.home)
    script_file = '{}.py'.format(name)
    script_path = '{}/{}'.format(direction, script_file)
    if os.path.exists(script_path):
        click.echo("Removing script {}".format(script_path))
        os.remove(script_path)
# _____________________________________________


def rename_script(migration, direction, old_name, new_name):
    os.chdir(migration.home)
    old_script_file = '{}.py'.format(old_name)
    old_script_path = '{}/{}'.format(direction, old_script_file)

    if os.path.exists(old_script_path):
        new_script_file = '{}.py'.format(new_name)
        new_script_path = '{}/{}'.format(direction, new_script_file)
        click.echo("Renaming script {} to {}".format(old_script_path, new_script_path))
        os.rename(old_script_path, new_script_path)
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


def plan_record_exists(dba, migration, name):
    changeid = dba.fetch_planned_changeid_by_name(name)
    return changeid
# _____________________________________________


def populate_plan_table(dba, plan):
    click.echo("Sync plan file into DB metaschema plan table")
    changes = plan_file_entries(plan)
    for change in changes:
        changeid = change['changeid']
        name = change['name']

        click.echo(message="+ {} {} ".format(name, '.' * (MSG_LENGTH - len(name))), nl=False)
        dba.apply_planned(changeid, name, change['msg'])
        click.echo(click.style('ok', fg='green'))
# _____________________________________________


def save_conf_path(conf_path):
    rootdir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
    with open(os.path.join(rootdir, CONF_PATH_FILE), 'w') as cp:
        cp.write(conf_path)
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


def validate_pgin_user(ctx, param, value):
    if value is None:
        raise click.BadParameter('--pgin_user command line parameter or PGIN_USER environment variable has to be set')

    return value
# _____________________________________________


def validate_project_topdir(ctx, param, value):
    if value is None:
        raise click.BadParameter(
            'PROJECT_DIR environment variable  or --project_dir command line parameter has to be set')

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
# _____________________________________________


def upgrade_plan_file(plan):
    changes = plan_file_entries(plan)
    for change in changes:
        change['changeid'] = generate_changeid()

    write_plan(plan, changes)

# ============= Commands ==================


@click.group()
@click.version_option(get_version())
@click.pass_context
def cli(ctx):
    """
    pgin is a command line tool for PostgreSQL DB migrations management.
    Run with Python 3.6+.
    Uses psycopg2 DB driver.
    """
    ctx.obj = Migration()
# _____________________________________________


@cli.command()
@click.argument('name')
@click.option('-m', '--msg', required=True, help="Short migration description")
@pass_migration
def add(migration, name, msg):
    """
    Adds migration script to the plan
    """

    os.chdir(migration.home)
    dba = connect_dba(migration)
    changeid = dba.fetch_planned_changeid_by_name(name)
    if changeid:
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

        changes = plan_file_entries(conf)

        for line in changes:
            changeid = line['changeid']
            if change_deployed(migration, changeid):
                continue

            name = line['name']
            deploy = get_change_deploy(migration, dba, name)

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
        sys.exit(1)
    except Exception:
        logger.exception('Exception in deploy')
        sys.exit(1)
    else:
        sys.exit(0)
    finally:
        disconnect_dba(dba)
# _____________________________________________


@cli.command()
@click.option(
    '-p',
    '--project',
    envvar='PGIN_PROJECT',
    callback=validate_project,
    help='Pgin project / DB name. If not provided, PGIN_PROJECT env variable value will be used.'
)
@click.option(
    '-u',
    '--dbuser',
    envvar='PGIN_USER',
    callback=validate_pgin_user,
    help='Pgin DB user. If not provided, PGIN_USER env variable value will be used'
)
@click.option(
    '-d',
    '--topdir',
    envvar='PGIN_TOPDIR',
    callback=validate_project_topdir,
    help='Pgin project top directory. If not provided, PGIN_TOPDIR env variable value will be used'
)
@click.option('--newdb', is_flag=True, required=False, help="If set to TRUE drops and re-creates existent DB")
def init(project, dbuser, topdir, newdb=False):
    """
        Initiates the project DB migrations.
    """

    conf = init_config(project, dbuser, topdir)
    init_db(conf, newdb)
# _____________________________________________


@cli.command()
@click.option('-y', '--yes', is_flag=True, callback=do_not_if_false, expose_value=False, prompt='Remove change?')
@click.argument('name', required=True)
@pass_migration
def remove(migration, name):
    """
    Adds migration script to the plan
    """

    try:
        dba = connect_dba(migration)
        os.chdir(migration.home)
        changeid = plan_record_exists(dba, migration, name)
        if not changeid:
            click.echo("Change {} not found in migration plan".format(name))
            sys.exit(0)

        if change_deployed(migration, changeid):
            click.echo("Cannot remove a deployed change {}. Revert first".format(name))
            sys.exit(1)

        click.echo("Removing change %s from migration plan" % name)
        remove_from_plan(migration, changeid)
        dba.remove_change_from_plan(changeid)
    finally:
        disconnect_dba(dba)

    for direction in ['deploy', 'revert']:
        remove_script(migration, direction, name)
# _____________________________________________


@cli.command()
@click.option('-y', '--yes', is_flag=True, callback=do_not_if_false, expose_value=False, prompt='Rename change?')
@click.argument('old_name', required=True)
@click.argument('new_name', required=True)
@pass_migration
def rename(migration, old_name, new_name):
    """
    Rename change name
    """

    try:
        dba = connect_dba(migration)
        os.chdir(migration.home)
        changeid = plan_record_exists(dba, migration, old_name)
        if not changeid:
            click.echo("Change {} not found in migration plan".format(old_name))
            sys.exit(0)

        click.echo("Renaming change {} to {}".format(old_name, new_name))
        rename_in_plan(migration, changeid, old_name, new_name)
        dba.rename_change_in_plan(changeid, new_name)
    finally:
        disconnect_dba(dba)

    for direction in ['deploy', 'revert']:
        rename_script(migration, direction, old_name, new_name)
# _____________________________________________


@cli.command()
@click.option('-y', '--yes', is_flag=True, callback=do_not_if_false, expose_value=False, prompt='Revert?')
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
            if not change_deployed(migration, line['changeid']):
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
@click.option('-y', '--yes', is_flag=True, callback=do_not_if_false, expose_value=False, prompt='Remove tag?')
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
