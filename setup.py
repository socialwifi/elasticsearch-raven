from setuptools import setup

setup(
    name='Elasticsearch-Raven',
    version='1.0',
    author='Tomasz Wysocki',
    author_email='tomasz@pozytywnie.pl',
    url='https://github.com/pozytywnie/elasticsearch-raven/',
    packages=['elasticsearch_raven'],
    scripts=['bin/elasticsearch-raven.py'],
    license='MIT',
    description='Proxy that allows to send logs from Raven to Elasticsearch.',
    long_description=open('README.md').read(),
    install_requires=['elasticsearch'],
    test_suite='tests',
)
