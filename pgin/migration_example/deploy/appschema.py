from lib.basemigration import Basemigration
# =========================================


class Appschema(Basemigration):
    """
    Migration deploy/appschema
    """
    def __call__(self):

        """
        Migration deploy/appschema
        """

        query = """
            CREATE SCHEMA %s
        """

        params = [self.project]
        try:
            self.cursor.execute(query, params)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        # ______________________________________


# ==============================

def deploy(conn):
    Appschema(conn)()
