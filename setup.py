#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages
from cassnap_manage.py import __version__

setup(
    name='cassnap_manage',
    version=__version__,
    packages=find_packages(),
    author="Pierre Mavro",
    author_email="pierre@mavro.fr",
    description="S Export and manage Cassandra snapshots to hadoop",
    long_description=open('README.md').read(),
    install_requires=open('requirements.txt').read().splitlines(),
    include_package_data=True,
    url='https://github.com/deimosfr/cassandra_snap_to_hadoop',
    classifiers=[
        "Programming Language :: Python",
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: GNU General Public License v2 or later (GPLv2+)",
        "Environment :: Console",
        "Natural Language :: English",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 2.7",
        "Topic :: Communications",
    ],
)
