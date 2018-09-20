from distutils.core import setup

setup(
    name='hassutils',
    version='0.0.1',
    author='Joe Smallman',
    author_email='ijsmallman@gmail.com',
    description='Utilities for home-assistant',
    packages=[
      'hassutils.database',
      'hassutils.utils'
    ]
)
