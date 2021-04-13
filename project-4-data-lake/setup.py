from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='datalake_ingestion',
    version='1.0.0',
    author='Ernani de Britto Murtinho',
    author_email='ernanibmurtinho@gmail.com',
    packages=find_packages(),
    package_data={'datalakeingestion': ['docs']},
    long_description=long_description,
    url='',
    license='',
    description='datalake: Datalake ingestion',
    python_requires='>=3.7',
)