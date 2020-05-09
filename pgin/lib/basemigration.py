import psycopg2
# ============================


class Basemigration:
    def __init__(self, project, project_user, conf, conn, logger):
        self.project = project
        self.project_user = project_user
        self.conf = conf
        self.logger = logger
        self.conn = conn
        self.cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
