from lib.basemigration import Basemigration
# =========================================


class Appchema(Basemigration):
    """
    Migration deploy/appchema
    """
    def __call__(self):

        """
        Migration deploy/appchema
        """

        query = """
        """

        params = []
        try:
            self.cursor.execute(query, params)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        # ______________________________________


# ==============================

def deploy(conn):
    Appchema(conn)()
