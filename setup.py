from setuptools import setup, find_packages

setup(
    name='rdsslib',
    version='0.1.0',
    author='JISC RDSS',
    description='A repository of shared libraries',
    install_requires=[
        'boto3',
    ],
    tests_require=[
        'pytest',
    ],
    packages=['rdsslib']
)
