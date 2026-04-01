#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-intercom',
      version='2.0.1',
      description='Singer.io tap for extracting data from the Intercom API',
      author='jeff.huth@bytecode.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_intercom'],
      install_requires=[
          'backoff==1.8.0',
          'requests>=2.25.1,<3.0.0',
          'pipelinewise-singer-python==1.2.0',
          'hotglue-singer-sdk>=1.0.15,<2.0.0'
      ],
      entry_points='''
          [console_scripts]
          tap-intercom=tap_intercom:main
      ''',
      packages=find_packages(),
      package_data={
          'schemas': ['tap_intercom/schemas/*.json']
       },
       include_package_data=True,
      extras_require={
          'dev': [
              'pylint==2.14.5',
              'ipdb',
              'nose',
              'parameterized'
          ]
      }
      )
