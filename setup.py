import os
from setuptools import setup
APP = 'postmig'
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
    description='Rather primitive PostgreSQL migration facility',
    install_requires=[
        'jinja2',
    ],

    entry_points='''
        [console_scripts]
        postmig=postmig.scripts.pgmig:cli
    ''',
)
