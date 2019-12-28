from lib.basemigration import Basemigration
# =========================================


class Appschema(Basemigration):

    def __call__(self):

        """
        Migration deploy/appschema
        """

        query = """
            CREATE SCHEMA IF NOT EXISTS %s
        """

        params = [self.project]
        try:
            self.cursor.execute(query, params)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        # ______________________________________
