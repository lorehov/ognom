#!/usr/bin/env python
import os
import sys
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

from ognom import __version__


def read(fname):
    try:
        return open(os.path.join(os.path.dirname(__file__), fname)).read()
    except IOError:
        return ""


class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', 'Arguments to pass to py.test')]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = ['tests/']

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)

setup(
    name='ognom',
    description='Neat ODM wrapper around great PyMongo driver',
    long_desccription=read('README.md'),

    author='Lev Orekhov',
    author_email='lev.orekhov@gmail.com',
    url='https://github.com/lorehov/ognom',

    maintainer='Lev Orekhov',
    maintainer_email='lev.orekhov@gmail.com',


    version=__version__,
    packages=find_packages(),

    tests_require=['pytest'],
    install_requires=['pymongo<3.0', 'python-dateutil', 'six'],
    cmdclass={'test': PyTest},

    keywords='mongo mongodb pymongo orm odm',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    license='MIT',
)
