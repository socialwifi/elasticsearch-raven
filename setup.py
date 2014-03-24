from setuptools import setup
from pip.req import parse_requirements

requirements = parse_requirements('requirements.txt')
install_requires = [str(requirement.req) for requirement in requirements]

setup(
    name='Elasticsearch-Raven',
    version='0.1.0',
    author='',
    author_email='',
    packages=['elasticsearch_raven'],
    scripts=['bin/elasticsearch_raven.py'],
    license='LICENSE',
    description='Proxy that allows to log from raven to elasticsearch.',
    long_description=open('README.md').read(),
    install_requires=install_requires,
)
