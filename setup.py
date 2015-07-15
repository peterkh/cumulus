#!/usr/bin/env python
from ez_setup import use_setuptools
use_setuptools()

from setuptools import setup, find_packages

setup(
    name="cumulus",
    version="0.3a0",
    author="Peter Hall",
    author_email='cumulus@peterkh.net',
    license='Apache Software License 2.0',
    description='Manages AWS Cloudformation stacks across multiple CF'
                ' templates',
    packages=find_packages(),
    include_package_data=True,
    install_requires=['PyYAML', 'argparse', 'boto', 'simplejson', 'pystache'],
    entry_points={
        'console_scripts': [
            'cumulus=cumulus:main']}
)
