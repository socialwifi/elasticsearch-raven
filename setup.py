from setuptools import setup


setup(
    name='Elasticsearch-Raven',
    version='0.1.0',
    author='Tomasz Wysocki',
    author_email='tomasz@pozytywnie.pl',
    packages=['elasticsearch_raven'],
    scripts=['bin/elasticsearch-raven.py'],
    license='LICENSE',
    description='Proxy that allows to log from raven to elasticsearch.',
    long_description=open('README.md').read(),
    install_requires=['elasticsearch'],
    test_suite='tests'
)
