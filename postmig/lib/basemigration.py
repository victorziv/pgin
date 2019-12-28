import psycopg2
# ============================


class Basemigration:
    def __init__(self, conn):
        self.conn = conn
        self.cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
