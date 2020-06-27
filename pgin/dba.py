import logging
import datetime
import psycopg2
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
        self.dburi = Config.db_connection_uri(dbname=dbname, dbuser=dbuser)
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

    def apply_planned(self, changeid, change, msg):
        query = """
            INSERT INTO %s.plan
            (changeid, name, planned, msg)
            VALUES
            (%s, %s, %s, %s)
            ON CONFLICT(changeid)
            DO NOTHING
        """
        params = [AsIs(self.meta_schema), changeid, change, datetime.datetime.utcnow(), msg]

        self.cursor.execute(query, params)
        self.conn.commit()
    # _____________________________

    def apply_tag(self, changeid, tag, msg):
        query = """
            UPDATE %s.plan
            SET
                tag = %s,
                tagmsg = %s,
                tagged = %s
            WHERE changeid = %s
        """
        params = [
            AsIs(self.meta_schema),
            tag,
            msg,
            datetime.datetime.utcnow(),
            changeid
        ]

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

        self.logger.debug("Creating DB %s with owner %s", newdb, newdb_owner)

        try:
            admin_db_uri = Config.db_connection_uri_admin(dbuser=newdb_owner)
            self.logger.debug("Admin DB URI: %r", admin_db_uri)
            admin_conn = self.connectdb(admin_db_uri)
            admin_cursor = admin_conn.cursor()
            admin_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            # Create DB
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
           CREATE TABLE IF NOT EXISTS %(meta_schema)s.changes (
               changeid uuid PRIMARY KEY,
               name VARCHAR(256) UNIQUE REFERENCES %(meta_schema)s.plan(name),
               applied TIMESTAMP WITHOUT TIME ZONE DEFAULT NULL,
               FOREIGN KEY(changeid) REFERENCES %(meta_schema)s.plan(changeid) ON UPDATE CASCADE
           )
        """
        params = {'meta_schema': AsIs(self.meta_schema)}
        self.cursor.execute(query, params)
        self.conn.commit()
    # _____________________________

    def create_plan_table(self):
        query = """
           CREATE TABLE IF NOT EXISTS %s.plan (
               changeid uuid PRIMARY KEY,
               name VARCHAR(256) UNIQUE,
               planned TIMESTAMP WITHOUT TIME ZONE DEFAULT NULL,
               msg TEXT,
               tag VARCHAR(100) UNIQUE,
               tagmsg TEXT,
               tagged TIMESTAMP WITHOUT TIME ZONE DEFAULT NULL
           )
        """
        params = [AsIs(self.meta_schema)]
        self.cursor.execute(query, params)
        self.conn.commit()
    # _____________________________

    def create_tags_table(self):
        query = """
           CREATE TABLE IF NOT EXISTS %s.tags (
               changeid uuid PRIMARY KEY,
               tag VARCHAR(100) UNIQUE,
               msg TEXT,
               tagged TIMESTAMP WITHOUT TIME ZONE DEFAULT NULL,
               FOREIGN KEY(changeid) REFERENCES %s.plan(changeid) ON UPDATE CASCADE
           )
        """
        params = [AsIs(self.meta_schema), AsIs(self.meta_schema)]
        self.cursor.execute(query, params)
        self.conn.commit()
    # _____________________________

    def connectdb(self, dburi=None):
        if dburi is None:
            dburi = self.dburi
        return psycopg2.connect(dburi)
    # ___________________________

    def drop_other_connections(self, dbname):
        query = '''
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = %s
            AND pid <> pg_backend_pid();
        '''
        params = [dbname]
        self.cursor.execute(query, params)
        self.conn.commit()
    # ___________________________

    def dropdb(self, db_to_drop=None):
        """
        """

        if db_to_drop is None:
            db_to_drop = self.dbname

        self.logger.info("Dropping DB %s", db_to_drop)

        try:
            admin_db_uri = Config.db_connection_uri_admin(dbuser=self.dbuser)
            self.logger.debug("Admin DB URI: %r", admin_db_uri)
            admin_conn = self.connectdb(admin_db_uri)
            admin_cursor = admin_conn.cursor()
            admin_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            query = """DROP DATABASE IF EXISTS %(dbname)s"""
            params = {'dbname': AsIs(db_to_drop)}
            admin_cursor.execute(query, params)
        finally:
            admin_cursor.close()
            admin_conn.close()
    # ___________________________

    def fetch_deployed_changes(self, offset=0, limit=None):
        query = """
            SELECT
                changeid,
                name
            FROM %s.changes
            ORDER BY applied DESC
            OFFSET %s
        """
        params = [AsIs(self.meta_schema), offset]

        if limit:
            query += 'LIMIT %s'
            params.append(limit)

        self.cursor.execute(query, params)
        fetch = self.cursor.fetchall()
        if fetch is None:
            return []

        return [dict(f) for f in fetch]
    # ___________________________

    def fetch_last_deployed_change(self):
        query = """
            SELECT
                changeid,
                name,
                applied

            FROM %s.changes
            ORDER BY applied DESC
            LIMIT 1
        """
        params = [AsIs(self.meta_schema)]

        self.cursor.execute(query, params)
        fetch = self.cursor.fetchone()
        if fetch is None:
            return

        return dict(fetch)
    # ___________________________

    def fetch_deployed_changeid_by_name(self, change):
        query = """
            SELECT changeid
            FROM %s.changes
            WHERE name = %s
        """
        params = [AsIs(self.meta_schema), change]

        self.cursor.execute(query, params)
        fetch = self.cursor.fetchone()
        if fetch is None:
            return

        return dict(fetch)['changeid']
    # ___________________________

    def fetch_planned_changeid_by_name(self, change):
        query = """
            SELECT changeid
            FROM %s.plan
            WHERE name = %s
        """
        params = [AsIs(self.meta_schema), change]

        self.cursor.execute(query, params)
        fetch = self.cursor.fetchone()
        if fetch is None:
            return

        return dict(fetch)['changeid']
    # ___________________________

    def fetch_change_by_tag(self, tag):
        query = """
            SELECT name AS change
            FROM %s.plan
            WHERE tag = %s
        """
        params = [AsIs(self.meta_schema), tag]

        self.cursor.execute(query, params)
        fetch = self.cursor.fetchone()
        if fetch is None:
            return

        return dict(fetch)['change']
    # ___________________________

    def fetch_tags(self):
        query = """
            SELECT
                tag,
                tagmsg,
                name AS change
            FROM  %s.plan
            WHERE tag is not NULL
            ORDER BY tag
        """
        params = [AsIs(self.meta_schema)]

        self.cursor.execute(query, params)
        fetch = self.cursor.fetchall()
        if fetch is None:
            return []

        return [dict(f) for f in fetch]
    # ___________________________

    def grant_connect_to_db(self, dbname=None, dbuser=None):
        if dbname is None:
            dbname = self.dbname

        if dbuser is None:
            dbuser = self.dbuser

        try:
            dburi = Config.db_connection_uri(dbname, dbuser)
            conn = self.connectdb(dburi)
            cursor = conn.cursor()
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            query = """
                GRANT CONNECT ON DATABASE %s TO %s
            """
            params = (AsIs(dbname), AsIs(dbuser))
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

    def set_search_path(self, schema):
        query = """
            SET search_path=%s,public
        """
        params = (AsIs(schema),)
        self.cursor.execute(query, params)
        self.conn.commit()
    # _____________________________

    def show_search_path(self):
        query = """SHOW search_path"""
        params = ()
        self.cursor.execute(query, params)
        fetch = self.cursor.fetchone()
        self.logger.debug("Pgin DBA: search path set to: %s", fetch)

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

    def remove_change_from_plan(self, change):
        query = """
            DELETE FROM %s.plan
            WHERE name = %s
        """
        params = [AsIs(self.meta_schema), change]

        self.cursor.execute(query, params)
        self.conn.commit()
    # _____________________________

    def remove_tag(self, change):
        query = """
            UPDATE %s.plan
            SET
                tag = NULL,
                tagmsg = NULL,
                tagged = NULL
            WHERE name = %s
        """
        params = [AsIs(self.meta_schema), change]

        self.cursor.execute(query, params)
        self.conn.commit()
    # _____________________________

    def revoke_connect_from_db(self, dbname=None, dbuser=None):
        conn = None
        cursor = None
        if dbname is None:
            dbname = self.dbname

        if dbuser is None:
            dbuser = self.dbuser

        try:
            dburi = Config.db_connection_uri(dbname, dbuser)
            conn = self.connectdb(dburi)
            cursor = conn.cursor()
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            query = """
                SELECT pg_terminate_backend(pg_stat_activity.pid)
                FROM pg_stat_activity
                WHERE pg_stat_activity.datname = %s
                AND pid <> pg_backend_pid();
            """
            params = (dbname, )
            cursor.execute(query, params)
            conn.commit()
        except psycopg2.OperationalError as e:
            if 'does not exist' in str(e):
                pass
            else:
                self.logger.exception("Operation error exception")

        except Exception:
            self.logger.exception("Revoke connection from db exception")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    # ___________________________________________
