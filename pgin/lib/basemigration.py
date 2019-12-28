import psycopg2
# ============================


class Basemigration:
    def __init__(self, project, conn):
        self.project = project
        self.conn = conn
        self.cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
