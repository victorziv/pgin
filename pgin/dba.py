import logging
import datetime
import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, AsIs
from config import Config
# ==============================================================


class DBAdmin:

    def __init__(self, conf, dbname, dbuser):
        self.conf = conf
        self.logger = logging.getLogger(conf['PROJECT'])
        self.dbname = dbname
        self.dbuser = dbuser
        self.meta_schema = 'pgin'
    # __________________________________________

    def apply_change(self, changeid, change):
        query = """
            INSERT INTO %s.changes
            (changeid, name, applied)
            VALUES
            (%s, %s, %s)
            ON CONFLICT(changeid)
            DO NOTHING
        """
        params = [AsIs(self.meta_schema), changeid, change, datetime.datetime.utcnow()]

        self.cursor.execute(query, params)
        self.conn.commit()

    # _____________________________

    def createdb(self, newdb=None, newdb_owner=None):
        """
        """

        if newdb is None:
            newdb = self.dbname

        if newdb_owner is None:
            newdb_owner = self.dbuser

        self.logger.info("Creating DB %s with owner %s", newdb, newdb_owner)

        try:
            admin_db_uri = Config.db_connection_uri_admin(dbuser=newdb_owner)
            self.logger.info("Admin DB URI: %r", admin_db_uri)
            admin_conn = self.connectdb(admin_db_uri)
            admin_cursor = admin_conn.cursor()
            admin_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            # Create DB
            query = """CREATE DATABASE %(dbname)s WITH OWNER %(user)s"""
            params = {'dbname': AsIs(newdb), 'user': AsIs(newdb_owner)}
            admin_cursor.execute(query, params)

            # Set search_path
            query = """
                ALTER DATABASE %(dbname)s
                SET search_path TO %(dbname)s,public;
            """
            params = {'dbname': AsIs(newdb), 'user': AsIs(newdb)}
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

    def create_plan_table(self):
        query = """
           CREATE TABLE IF NOT EXISTS %s.plan (
               changeid CHAR(40) PRIMARY KEY,
               name VARCHAR(100) UNIQUE,
               planned TIMESTAMP WITHOUT TIME ZONE DEFAULT NULL
           );
        """
        params = [AsIs(self.meta_schema)]
        self.cursor.execute(query, params)
        self.conn.commit()
    # _____________________________

    def create_changes_table(self):
        query = """
           CREATE TABLE IF NOT EXISTS %s.changes (
               changeid CHAR(40) PRIMARY KEY,
               name VARCHAR(100) UNIQUE,
               applied TIMESTAMP WITHOUT TIME ZONE DEFAULT NULL
           );
        """
        params = [AsIs(self.meta_schema)]
        self.cursor.execute(query, params)
        self.conn.commit()
    # _____________________________

    def connectdb(self, dburi):
        try:
            conn = psycopg2.connect(dburi)
            return conn

        except psycopg2.OperationalError as e:
            if 'does not exist' in str(e):
                self.logger.exception("OOPS: {}".format(e))
                return
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

    def dropdb(self, dbname=None):
        if dbname is None:
            dbname = self.conf['DBNAME']

        self.logger.info("Dropping DB: {}".format(dbname))
        try:
            admin_conn = self.connectdb(self.conf['DB_CONN_URI_ADMIN'])
            admin_cursor = admin_conn.cursor()
            admin_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.disconnect_all_from_db(admin_cursor, dbname)

            query = """DROP DATABASE IF EXISTS %(dbname)s"""
            params = {'dbname': AsIs(dbname)}
            admin_cursor.execute(query, params)
        finally:
            admin_cursor.close()
            admin_conn.close()
    # ___________________________

    def fetch_deployed_changes(self):
        query = """
            SELECT
                changeid,
                name
            FROM %s.changes
            ORDER BY applied DESC
        """
        params = [AsIs(self.meta_schema)]

        self.cursor.execute(query, params)
        fetch = self.cursor.fetchall()
        if fetch is None:
            return []

        return [dict(f) for f in fetch]
    # ___________________________

    def grant_connect_to_db(self):
        try:
            conn = self.connectdb(self.conf['DB_CONN_URI_ADMIN'])
            cursor = conn.cursor()
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
        self.conn = self.connectdb(app.config['DB_CONN_URI'])
        self.cursor = self.conn.cursor(cursor_factory=DictCursor)
        self.show_search_path()
        app.db = self
        return app
    # _____________________________

    def set_search_path(self, schema):
        query = """
            ALTER DATABASE %s
            SET search_path=%s,public
        """
        params = (AsIs(schema), AsIs(schema))
        self.cursor.execute(query, params)
        self.conn.commit()
    # _____________________________

    def show_search_path(self):
        query = """SHOW search_path"""
        params = ()
        self.cursor.execute(query, params)
        fetch = self.cursor.fetchone()
        self.logger.debug("Search path fetch: %s", fetch)

        return fetch['search_path']
    # _____________________________

    def remove_change(self, changeid):
        query = """
            DELETE FROM %s.changes
            WHERE changeid = %s
        """
        params = [AsIs(self.meta_schema), changeid]

        self.cursor.execute(query, params)
        self.conn.commit()
    # _____________________________

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

    def revoke_connect_from_db(self, dbname, dbuser):
        try:
            dburi = Config.db_connection_uri(dbname, dbuser)
            conn = self.connectdb(dburi)
            cursor = conn.cursor()
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
