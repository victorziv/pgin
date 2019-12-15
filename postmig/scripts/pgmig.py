import os
import sys

import click
from config import Configurator
conf = Configurator.configure()
logger = Configurator.set_logging(name=conf['PROJECT'], console_logging=True)
from lib.oshelper import Oshelper  # noqa
# =====================================


class Migration(object):

    def __init__(self, home):
        self.project = conf['PROJECT']
        self.home = home
        self.config = {}
        self.verbose = False
        self.oshelper = Oshelper(logger)
    # ___________________________________

    def set_config(self, key, value):
        self.config[key] = value
        if self.verbose:
            click.echo('  config[%s] = %s' % (key, value), file=sys.stderr)

    def __repr__(self):
        return '<Repo %r>' % self.home
# _____________________________________________


pass_migration = click.make_pass_decorator(Migration)


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
    migration.oshelper.create_directory(migration.home)
    for d in ['deploy', 'revert']:
        migration.oshelper.create_directory(os.path.join(migration.home, d))


@cli.command()
@click.confirmation_option()
@pass_migration
def delete(repo):
    """Deletes a repository.

    This will throw away the current repository.
    """
    click.echo('Destroying repo %s' % repo.home)
    click.echo('Deleted!')


@cli.command()
@click.option('--username', prompt=True,
              help='The developer\'s shown username.')
@click.option('--email', prompt='E-Mail',
              help='The developer\'s email address')
@click.password_option(help='The login password.')
@pass_migration
def setuser(repo, username, email, password):
    """Sets the user credentials.

    This will override the current user config.
    """
    repo.set_config('username', username)
    repo.set_config('email', email)
    repo.set_config('password', '*' * len(password))
    click.echo('Changed credentials.')


@cli.command()
@click.option('--message', '-m', multiple=True,
              help='The commit message.  If provided multiple times each '
              'argument gets converted into a new line.')
@click.argument('files', nargs=-1, type=click.Path())
@pass_migration
def commit(repo, files, message):
    """Commits outstanding changes.

    Commit changes to the given files into the repository.  You will need to
    "repo push" to push up your changes to other repositories.

    If a list of files is omitted, all changes reported by "repo status"
    will be committed.
    """
    if not message:
        marker = '# Files to be committed:'
        hint = ['', '', marker, '#']
        for file in files:
            hint.append('#   U %s' % file)
        message = click.edit('\n'.join(hint))
        if message is None:
            click.echo('Aborted!')
            return
        msg = message.split(marker)[0].rstrip()
        if not msg:
            click.echo('Aborted! Empty commit message')
            return
    else:
        msg = '\n'.join(message)
    click.echo('Files to be committed: %s' % (files,))
    click.echo('Commit message:\n' + msg)


@cli.command(short_help='Copies files.')
@click.option('--force', is_flag=True,
              help='forcibly copy over an existing managed file')
@click.argument('src', nargs=-1, type=click.Path())
@click.argument('dst', type=click.Path())
@pass_migration
def copy(repo, src, dst, force):
    """Copies one or multiple files to a new location.  This copies all
    files from SRC to DST.
    """
    for fn in src:
        click.echo('Copy from %s -> %s' % (fn, dst))
