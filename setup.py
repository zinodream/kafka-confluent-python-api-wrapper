'''
1. pip install list
pip install setuptools
pip install wheel
2. build package (save dist folder)
python setup.py bdist_wheel
'''
from setuptools import setup, find_packages
from version import __version__

setup(
    name='kafka-producer-api',
    version=__version__,
    description='kafka-producer-api-python',
    author='mococo',
    author_email='dev@mococo.co.kr',
    url='',
    python_requires='>=3.5',
    packages= [''],
    install_requires=[
        'confluent-kafka',
        'random-string',
        'pytz'
    ],
    license='',
    classifiers=[
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)