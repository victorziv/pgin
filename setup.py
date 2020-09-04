import os
from setuptools import setup, find_packages
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
    author="Victor Ziv",
    author_email="vziv@infinidat.com",
    url='https://git.infinidat.com/ivt/%s.git' % APP,
    packages=find_packages(),
    license='Proprietary',
    description='Very dedicated PostgreSQL DB migration utility. Using Python 3.6+ and psycopg2',
    long_description=open('README.rst').read(),
    long_description_content_type="text/x-rst",
    install_requires=[
        'jinja2',
        'psycopg2',
        'click',
        'jsonlines',
        'colorama',
        'tabulate',
        'flake8'
    ],
    include_package_data=True,
    python_requires='>=3.6',

    entry_points='''
        [console_scripts]
        pgin=pgin.scripts.pgin:cli
    ''',
)
