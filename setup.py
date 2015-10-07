from setuptools import setup

v = '1.0'

setup(
    name='epf',
    version=v,
    description='Apple EPF crawler, downloader and parser',
    author='Andrei Fokau',
    author_email='andrei@5monkeys.se',
    url='https://github.com/andreif/epf',
    download_url='https://github.com/andreif/epf/tarball/' + v,
    license='MIT',
    packages=['epf'],
)
