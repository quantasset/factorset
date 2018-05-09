#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst', encoding='utf-8') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = ['Click>=6.0', 'aiohttp>=3', 'grequests>=0.3.0', 'pandas>=0.20.0','lxml>=3'
                'requests>=2', 'beautifulsoup4>=4', 'tushare>=1.1.6', 'fake-useragent>=0.1.10', 'redis>=2.10.6',
                ]

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest', ]

setup(
    author="Wenchao Zhang",
    author_email='495673131@qq.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
		'Topic :: Scientific/Engineering'
    ],
    description="A Python package for computing chinese financial factors.",
    entry_points={
        'console_scripts': [
            'factorset=factorset.cli:main',
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='factorset',
    name='factorset',
    packages=find_packages(include=['factorset']),
    package_data = {
        'factorset': ['data/*.csv'],
    },
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/quantasset/factorset',
    version='0.0.2',
    zip_safe=False,
)
