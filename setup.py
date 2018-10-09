#!/usr/bin/env python3
from setuptools import setup, find_packages

setup(
    name="s3etag",
    version="0.0.1",
    packages=find_packages(exclude='test'),
    install_requires=[
        'boto3',
        'humanfriendly'
    ],
    license="GPL",
    entry_points={
        'console_scripts': [
            's3etag = s3etag.main:main'
        ]
    }
)
