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
    PROJECT = 'postmig'

    PROJECTDIR = os.path.abspath(os.path.dirname(__file__))
    ROOTDIR = os.path.dirname(PROJECTDIR)
    LOGDIR = os.path.join(ROOTDIR, 'logs')
    DOCDIR = os.path.join(ROOTDIR, 'docs')

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
    DBNAME_ADMIN = 'postgres'
    DB_CONN_URI_ADMIN = DB_URI_FORMAT.format(
        dbname=DBNAME_ADMIN,
        **DB_CONNECTION_PARAMS
    )
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


class DevelopmentConfig(Config):
    SERVER_NAME = 'localhost:7000'
    ENV = 'develop'
    DEBUG = figure_debug_mode('develop')
    DBNAME = "%s%s" % (Config.PROJECT, ENV)
    DB_CONN_URI = Config.DB_URI_FORMAT.format(
        dbname=DBNAME,
        **Config.DB_CONNECTION_PARAMS
    )
    MAIL_SERVER = 'smtp-dev.lab.il.infinidat.com'
    MAIL_PORT = 25
    MAIL_USE_TLS = False
    MAIL_USERNAME = None
    MAIL_PASSWORD = None
    ELASTICSEARCH_URI = 'http://localhost:9200'
# ===================================


class TestingConfig(Config):
    SERVER_NAME = 'localhost.localdomain:7000'
    LOGSERVER = 'localhost'
    LOGSERVER_HTTP = LOGSERVER
    ENV = 'testing'
    DEBUG = figure_debug_mode(ENV)
    DBNAME = "%s%s" % (Config.PROJECT, ENV)
    DB_CONN_URI = Config.DB_URI_FORMAT.format(
        dbname=DBNAME,
        **Config.DB_CONNECTION_PARAMS
    )
# ===================================


class ProductionConfig(Config):
    SERVER_NAME = 'hwinfosrv.lab.il.infinidat.com'
    DBNAME = Config.PROJECT
    DB_CONN_URI = Config.DB_URI_FORMAT.format(
        dbname=DBNAME,
        **Config.DB_CONNECTION_PARAMS
    )
    TASK_MNG_PORT = 29955
    TASK_SINK_PORT = 29956
    TASK_EXECUTOR_PORT = 29957
    TASK_LOG_PORT = 29958

    IVT_TEAM_MAIL_RECIPIENT = 'ivt.team@infinidat.com'
    HWINFRA_TEAM_MAIL_RECIPIENT = 'hardware.infra@infinidat.com'

# ===================================


class StagingConfig(Config):
    SERVER_NAME = 'hwinfo-staging.lab.il.infinidat.com'
    DBNAME = Config.PROJECT
    DB_CONN_URI = Config.DB_URI_FORMAT.format(
        dbname=DBNAME,
        **Config.DB_CONNECTION_PARAMS
    )

    TASK_MNG_PORT = 27755
    TASK_SINK_PORT = 27756
    TASK_EXECUTOR_PORT = 27757
    TASK_LOG_PORT = 27758

# ===================================


cnf = {
    'develop': DevelopmentConfig,
    'testing': TestingConfig,
    'production': ProductionConfig,
    'staging': StagingConfig,
    'default': DevelopmentConfig
}
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
    def configure(cls, configkey='default'):
        config_name = os.getenv('%s_CONFIG' % Config.PROJECT) or configkey
        cnfcls = cnf[config_name]
        global conf
        conf = ConfiguratorDict()
        conf.from_cls(cnfcls)
        return conf
    # _________________________________

    @staticmethod
    def create_email_handler():
        auth = None
        if Config.MAIL_USERNAME or Config.MAIL_PASSWORD:
            auth = (Config.MAIL_USERNAME, Config.MAIL_PASSWORD)

        secure = None

        if Config.MAIL_USE_TLS:
            secure = ()

        mail_handler = SMTPHandler(
            mailhost=(Config.MAIL_SERVER, Config.MAIL_PORT),
            fromaddr=Config.MAIL_SENDER,
            toaddrs=Config.ADMINS,
            subject='%s Failure' % Config.PROJECT,
            credentials=auth,
            secure=secure
        )

        mail_handler.setLevel(logging.ERROR)
        return mail_handler
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
            logger.info("Log path: %s", logfile)

        return logger
    # ________________________________________

    @classmethod
    def log_to_console(cls, logging_level='INFO', out_to='stderr'):
        logformat = '%(asctime)s - %(levelname)-10s %(message)s'
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
