from setuptools import setup

setup(
    name='Elasticsearch-Raven',
    version='1.5.0',
    author='Tomasz Wysocki',
    author_email='tomasz@pozytywnie.pl',
    url='https://github.com/pozytywnie/elasticsearch-raven/',
    packages=['elasticsearch_raven'],
    scripts=['bin/elasticsearch-raven.py', 'bin/update_ids.py',
             'bin/udp_to_amqp.py', 'bin/amqp_to_elasticsearch.py'],
    license='MIT',
    description='Proxy that allows to send logs from Raven to Elasticsearch.',
    long_description=open('README.rst').read(),
    install_requires=['elasticsearch', 'kombu'],
    test_suite='tests',
)
