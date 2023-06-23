import os
from setuptools import (
    setup,
    find_packages
)

current_directory = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(current_directory, 'README.md'), "r") as readme:
    package_description = readme.read()

version_string = ""
with open (os.path.join(current_directory, ".version"), 'r') as version_file:
    version_string = version_file.read()

setup(
    name="mercury-sync",
    version=version_string,
    description="A pure-Python, asyncio based library for authoring distributed systems",
    long_description=package_description,
    long_description_content_type="text/markdown",
    author="Sean Corbett",
    author_email="sean.corbett@umontana.edu",
    url="https://github.com/scorbettUM/mercury-sync",
    packages=find_packages(),
    keywords=[
        'pypi', 
        'python',
        'distributed systems',
        'swim',
        'serf',
        'distributed',
    ],
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"
    ],
    install_requires=[
        'pydantic',
        'python3-dtls',
        'zstandard',
        'cryptography',
        'python-dotenv'
    ],
    python_requires='>=3.10'
)
