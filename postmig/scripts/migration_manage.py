#!/usr/bin/env python
import sys
import os
import click
PROJECT = 'postmig'
ROOTDIR = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
# ============================


class Migration:

    def __init__(self, home=None):
        if home is None:
            home = os.path.join(ROOTDIR, 'dbmigration')
        self.home = home
        self.config = {}
        self.verbose = False
    # _______________________

    def set_config(self, key, value):
        self.config[key] = value
        if self.verbose:
            click.echo("    config[%s] = %s" % (key, value), file=sys.stdr)
    # _______________________

    def create_directory(self, path):
        try:
            os.makedirs(path)
        except OSError as e:
            if e.errno != 17:
                raise
    # ____________________________

    def init(self):
        click.echo("Home: %r" % self.home)
        self.create_directory(self.home)
        for d in ['deploy', 'revert']:
            self.create_directory(os.path.join(self.home, d))
    # _______________________

    def __repr__(self):
        return "Migration directory: %r" % self.home

# =============================


pass_migration = click.make_pass_decorator(Migration)
# =============================


@click.group()
@click.option(
    '--home',
    envvar='MIGRATION_HOME',
    default=os.path.join(ROOTDIR, PROJECT, 'dbmigration'),
    metavar='PATH',
    help='Changes the default DB migrations path'
)
@click.option('--config', nargs=2, multiple=True, metavar='KEY VALUE', help='Overrides a config key/value pair.')
@click.option('--verbose', '-v', is_flag=True, help='Enables verbose mode.')
@click.version_option('1.0')
@click.pass_context
def cli(ctx, home, config, verbose):
    """Repo is a command line tool that showcases how to build complex
    command line interfaces with Click.
    This tool is supposed to look like a distributed version control
    system to show how something like this can be structured.
    """
    # Create a repo object and remember it as as the context object.  From
    # this point onwards other commands can refer to it by using the
    # @pass_migration decorator.
    ctx.obj = Migration()
    ctx.obj.verbose = verbose
    for key, value in config:
        ctx.obj.set_config(key, value)
# __________________________________________


@cli.command()
@click.argument('project', required=True, default=PROJECT)
@click.option('--uri', required=False, help='Optinal (repository) URI for the project')
@pass_migration
def init(migration, project, uri):
    """Clones a repository.
    This will clone the repository at SRC into the folder DEST.  If DEST
    is not provided this will automatically use the last path component
    of SRC and create that folder.
    """

    click.echo('Initiating migrations for project %s' % (project))
# ================================


def main():
    m = Migration()
    m.init()
# _________________________


if __name__ == '__main__':
    main()
