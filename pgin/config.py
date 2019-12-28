import os
import sys
import datetime
import logging
from lib.helpers import create_directory

logger = None
conf = None
# ==============================================


def get_version(directory):
    with open(os.path.join(directory, 'VERSION')) as fp:
        version = fp.read()

    return version.strip()
# _____________________________________________


class Config:
    LOGGER_NAME = 'postmig'
    PROJECT_USER = 'ivtapp'

    PROJECTDIR = os.path.abspath(os.path.dirname(__file__))
    ROOTDIR = os.path.dirname(PROJECTDIR)
    LOGDIR = os.path.join(ROOTDIR, 'logs')
    DOCDIR = os.path.join(ROOTDIR, 'docs')
    MIGRATIONS_PKG = 'migration'

    VERSION = get_version(PROJECTDIR)

    DBHOST = 'localhost'
    DBPORT = 5432
    DBUSER = PROJECT_USER
    DBPASSWORD = PROJECT_USER

    DB_CONNECTION_PARAMS = dict(
        dbhost=DBHOST,
        dbport=DBPORT,
        dbuser=DBUSER,
        dbpassword=DBPASSWORD
    )

    DB_URI_FORMAT = 'postgresql://{dbuser}:{dbpassword}@{dbhost}:{dbport}/{dbname}'
    # _____________________________

    @classmethod
    def db_connection_uri(cls, dbname):
        return cls.DB_URI_FORMAT.format(dbname=dbname, **cls.DB_CONNECTION_PARAMS)
    # _____________________________

    @classmethod
    def db_connection_uri_admin(cls):
        dbname_admin = 'postgres'
        return cls.DB_URI_FORMAT.format(dbname=dbname_admin, **cls.DB_CONNECTION_PARAMS)

    # _____________________________

    @classmethod
    def init_app(cls, app):
        from config import logger
        logger.debug("INIT APP RUN TYPE: %r", cls.__name__)
        logger.debug("INIT APP debug mode: %r", app.debug)

        handler_names = [h.__class__.__name__ for h in logger.handlers]
        logger.debug("INIT APP: Logging handler names before any additions: %r", handler_names)

        if app.debug:
            'StreamHandler' in handler_names or logger.addHandler(Configurator.log_to_console())
            logger.debug("ADD Console handler: %r", logger.handlers)
        elif Config.MAIL_SERVER:
            'SMTPHandler' in handler_names or logger.addHandler(Configurator.create_email_handler())
            logger.debug("ADD SMTP handler: %r", logger.handlers)
        else:
            logger.debug("No logger handlers additions: %r", logger.handlers)

# ===================================


class ConfiguratorDict(dict):

    def from_cls(self, cls):
        for key in dir(cls):

            if key.startswith('__'):
                continue

            if key.isupper():
                self[key] = getattr(cls, key)
# ===================================


class Configurator:

    @classmethod
    def configure(cls):
        global conf
        conf = ConfiguratorDict()
        conf.from_cls(Config)
        return conf
    # _________________________________

    @classmethod
    def set_logging_level(cls, logging_level):
        for h in cls.logger.handlers:
            h.setLevel(getattr(logging, logging_level.upper()))
    # _________________________________

    @classmethod
    def set_logging(cls, name, loglevel='DEBUG', console_logging=False, console_loglevel='INFO'):

        global logger
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, loglevel.upper()))
        logger.propogate = False
        logfile = os.path.join(Config.LOGDIR, '%s.log' % name)
        logger.addHandler(cls.log_to_file(name, logfile, loglevel))

        if console_logging:
            logger.addHandler(cls.log_to_console(console_loglevel))
            logger.debug("Log path: %s", logfile)

        return logger
    # ________________________________________

    @classmethod
    def log_to_console(cls, logging_level='INFO', out_to='stderr'):
#         logformat = '%(asctime)s - %(levelname)-10s %(message)s'
        logformat = '%(message)s'
        handler = logging.StreamHandler(getattr(sys, out_to))
        handler.setLevel(getattr(logging, logging_level.upper()))
        handler.setFormatter(logging.Formatter(logformat))
        return handler
    # ___________________________________________

    @classmethod
    def log_to_file(cls, name, logfile, loglevel, logformat=None):
        if logformat is None:
            logformat = '%(asctime)s %(process)d %(levelname)-10s %(module)s %(funcName)-4s %(message)s'

        create_directory(os.path.dirname(logfile))

        handler = logging.FileHandler(logfile)

        handler.setLevel(getattr(logging, loglevel.upper()))
        handler.setFormatter(logging.Formatter(logformat))

        return handler

# ===============================================


PARAMS = {
    'version': Config.VERSION,
    'current_year': datetime.datetime.utcnow().year

}
