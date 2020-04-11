import os
from setuptools import setup
APP = 'pgin'
here = os.path.abspath(os.path.dirname(__file__))
# ==========================


def read_version():
    with open(os.path.join(here, APP, 'VERSION')) as vfr:
        version = vfr.read().strip()
    return version
# ______________________________


setup(
    name=APP,
    version=read_version(),
    py_modules=[APP],
    description='Very dedicated PostgreSQL DB migration utility. Using Python 3.6+ and psycopg2',
    install_requires=[
        'jinja2',
        'psycopg2',
        'click',
        'jsonlines',
        'colorama',
        'tabulate'
    ],

    entry_points='''
        [console_scripts]
        pgin=pgin.scripts.pgin:cli
    ''',
)
