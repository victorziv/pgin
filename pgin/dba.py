import sys
import os
import datetime
import glob
import importlib
import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, AsIs
from config import Config, logger  # noqa
# ========================================


class DBAdmin:

    def __init__(self, conf, dbname):
        self.logger = logger
        self.conf = conf
        self.dbname = dbname
        self.meta_schema = 'pgin'
        self.createdb(newdb=dbname, newdb_owner=conf['PROJECT_USER'])
        dburi = Config.db_connection_uri(dbname)
        self.conn, self.cursor = self.connectdb(dburi)
    # __________________________________________

    def already_applied(self, cursor, version):
        self.logger.debug("Check version applied: %r", version)

        query = """
            SELECT EXISTS(
                SELECT 1 FROM changelog WHERE version = %s
            )

        """
        params = (version,)

        cursor.execute(query, params)
        fetch = cursor.fetchone()
        self.logger.debug("Already applied: {}".format(fetch[0]))
        return fetch[0]
    # ___________________________

    def apply_versions(self, versions):
        self.logger.info("==== FOUND VERSIONS: %r", [v['version'] for v in versions])
        applied = []

        try:
            self.logger.info("==== CONNECTING TO URI: %r", self.conf['DB_CONN_URI'])
            conn, cursor = self.connectdb(self.conf['DB_CONN_URI'])

            for ver in versions:
                if self.already_applied(cursor, ver['version']):
                    self.logger.info("Version %s already applied - skipping", ver['version'])
                    continue

                try:
                    module_name = ver['module']
                    mod = importlib.import_module('%s.%s' % (self.conf['MIGRATIONS_MODULE'], module_name))
                    mod.upgrade(conn, cursor)
                except Exception:
                    self.logger.exception("!! APPLY VERSIONS EXCEPTION")
                    conn.rollback()
                    raise
                else:
                    version = ver['version']
                    name = ver['name']
                    recordid = self.insert_changelog_record(version, name)
                    applied.append(version)
                    self.logger.info("Version %s applied", version)
                    self.logger.debug("Changelog record ID %s, version %s: %r", recordid, version, ver)

            if not len(applied):
                self.logger.info("No changes found for the DB")

        except Exception as e:
            self.logger.error('ERROR: %s; rolling back' % e)
            conn.rollback()
        finally:
            cursor.close()
            conn.close()
    # _____________________________

    def createdb(self, newdb, newdb_owner=None):
        """
        """
        self.logger.info("Creating DB %s with owner %s", newdb, newdb_owner)

        try:
            admin_conn, admin_cursor = self.connectdb(Config.db_connection_uri_admin())
            admin_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            query = """CREATE DATABASE %(dbname)s WITH OWNER %(user)s"""
            params = {'dbname': AsIs(newdb), 'user': AsIs(newdb_owner)}
            admin_cursor.execute(query, params)
        except psycopg2.ProgrammingError as pe:
            if 'already exists' in repr(pe):
                pass
            else:
                raise
        except Exception:
            raise
        finally:
            admin_cursor.close()
            admin_conn.close()
    # ___________________________________________

    def create_meta_schema(self):
        query = """
            CREATE SCHEMA IF NOT EXISTS %s
        """
        params = [AsIs(self.meta_schema)]
        self.cursor.execute(query, params)
        self.conn.commit()
    # ___________________________________________

    def create_changes_table(self):
        query = """
           CREATE TABLE IF NOT EXISTS %s.changes (
               changeid CHAR(128) PRIMARY KEY,
               name VARCHAR(100) UNIQUE,
               applied TIMESTAMP
           );
        """
        params = [AsIs(self.meta_schema)]
        self.cursor.execute(query, params)
        self.conn.commit()
    # _____________________________

    def connectdb(self, dburi):
        try:
            conn = psycopg2.connect(dburi)
            cursor = conn.cursor(cursor_factory=DictCursor)
            return conn, cursor

        except psycopg2.OperationalError as e:
            if 'does not exist' in str(e):
                self.logger.exception("OOPS: {}".format(e))
                return None, None
            else:
                raise
    # ___________________________

    def disconnect_all_from_db(self, cursor, dbname):
        query = """
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE pid <> pg_backend_pid()
            AND datname = %s
        """
        params = (dbname,)
        cursor.execute(query, params)
    # ___________________________________________

    def drop_table_changelog(self):
        query = """
            DROP TABLE IF EXISTS changelog;
        """
        params = {}

        self.cursor.execute(query, params)
        self.conn.commit()
    # _____________________________

    def downgradedb(self, db):
        try:
            self.conn, self.cursor = self.connectdb(self.conf['DB_CONN_URI'])
            migration_file = '0001.create_table-installationstep.sql'
            f = open(os.path.join(self.conf['MIGRATIONS_DIR'], migration_file))
            self.cursor.execute(f.read())
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            return
        finally:
            f.close()
            self.cursor.close()
            self.conn.close()
    # _____________________________

    def dropdb(self, dbname=None):
        if dbname is None:
            dbname = self.conf['DBNAME']

        self.logger.info("Dropping DB: {}".format(dbname))
        try:
            admin_conn, admin_cursor = self.connectdb(self.conf['DB_CONN_URI_ADMIN'])
            admin_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.disconnect_all_from_db(admin_cursor, dbname)

            query = """DROP DATABASE IF EXISTS %(dbname)s"""
            params = {'dbname': AsIs(dbname)}
            admin_cursor.execute(query, params)
        finally:
            admin_cursor.close()
            admin_conn.close()
    # ___________________________

    def grant_connect_to_db(self):
        try:
            conn, cursor = self.connectdb(self.conf['DB_CONN_URI_ADMIN'])
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            query = """
                GRANT CONNECT ON DATABASE %s TO %s
            """
            params = (AsIs(self.conf['DBNAME']), AsIs(self.conf['PROJECT_USER']))
            cursor.execute(query, params)
        finally:
            cursor.close()
            conn.close()

    # ___________________________________________

    def grant_access_to_table(self, table):
        query = """GRANT ALL ON TABLE %(table)s TO %(user)s"""
        params = {'table': AsIs(table), 'user': AsIs('ivt')}

        self.cursor.execute(query, params)
        self.conn.commit()
    # ___________________________

    def init_app(self, app):
        self.conn, self.cursor = self.connectdb(app.config['DB_CONN_URI'])
        self.show_search_path()
        app.db = self
        return app
    # _____________________________

    def show_search_path(self):
        query = """SHOW search_path"""
        params = ()
        self.cursor.execute(query, params)
        fetch = self.cursor.fetchone()
        self.logger.debug("Search path fetch: %s", fetch)

        return fetch['search_path']
    # _____________________________

    def insert_initial_data(self, app):
        app_context = app.app_context()
        app_context.push()
        from models import Role
        Role.insert_roles()
    # __________________________________

    def resetdb(self, dbname, logger=None):

        if logger is None:
            lg = self.logger
        else:
            lg = logger
        lg.info("Resetting DB: {}".format(dbname))

        self.revoke_connect_from_db()
        self.dropdb()
        self.createdb()
        self.grant_connect_to_db()
    # ___________________________

    def revoke_connect_from_db(self, dbname):
        try:
            dburi = Config.db_connection_uri(dbname)
            conn, cursor = self.connectdb(dburi)
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            query = """
                REVOKE CONNECT ON DATABASE %s FROM %s
            """
            params = (AsIs(dbname), AsIs(self.conf['PROJECT_USER']))
            cursor.execute(query, params)
        except psycopg2.ProgrammingError as e:
            if 'does not exist' in str(e):
                pass
            else:
                raise
        except Exception:
            self.logger.exception("Revoke connection from db exception")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    # ___________________________________________

    def upgradedb(self, upto_version):
#         conn, cursor = self.connectdb(self.conf['DB_CONN_URI'])
        self.logger.info("DB upgrade up to version: {}".format(upto_version))
        versions = self.get_upgrade_versions(upto_version)
        self.apply_versions(versions)
    # _____________________________

    def insert_changelog_record(self, version_number, name):

        try:
            conn, cursor = self.connectdb(self.conf['DB_CONN_URI'])

            query = """
                INSERT INTO changelog
                (version, name, applied)
                VALUES (%s, %s, %s)
                RETURNING id
            """
            params = (version_number, name, datetime.datetime.utcnow())

            cursor.execute(query, params)
            conn.commit()
            fetch = cursor.fetchone()
            return fetch['id']

        except Exception as e:
            self.logger.exception('ERROR: %s; rolling back' % e)
            conn.rollback()
            return
    # ____________________________

    def get_upgrade_versions(self, version):

        # --------------------------
        def _compose_version(vfile):
            module = os.path.splitext(os.path.basename(vfile))[0]
            version, name = module.split('_', 1)
            return dict(name=name, module=module, version=version)
        # --------------------------

        versions_path = os.path.join(self.conf['PROJECT_DIR'], self.conf['MIGRATIONS_DIR'])
        self.logger.debug("Versions path: {}".format(versions_path))
        vfiles = glob.iglob(os.path.join(versions_path, '[0-9]*.py'))
        versions = sorted(
            [_compose_version(vfile) for vfile in vfiles],
            key=lambda x: int(x['version'])
        )
        self.logger.debug("Versions: {}".format(versions))
        if version:
            return [v for v in versions if v['version'] == version]
        return versions
    # ___________________________

    def prompt(self, question):
        from distutils.util import strtobool

        sys.stdout.write('{} [y/n]: '.format(question))
        val = input()
        try:
            ret = strtobool(val)
        except ValueError:
            sys.stdout.write('Please answer with a y/n\n')
            return self.prompt(question)

        return ret
