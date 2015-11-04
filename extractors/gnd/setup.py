from setuptools import setup

setup(
    name="GndExtractor",
    version="1.0.0",
    author="Sören Oldag",
    author_email="soeren.oldag@student.hpi.de",
    license="GNU GPL v2+",
    packages=['gndextractor'],
    install_requires=[
        "argparse==1.3.0",
        "lxml==3.4.2",
        "psycopg2==2.6.1"
    ]
)
