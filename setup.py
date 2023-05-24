"""Package setup script.

This script handles everything about package publishing on PyPI.
"""

# Importing libraries
from setuptools import setup, find_packages

# Reading README.md for project description
with open("README.md", "r", encoding='utf-8') as f:
    __long_description__ = f.read()

# Setting up package information
setup(
    name='sparksnake',
    version='0.1.12',
    author='Thiago Panini',
    author_email='panini.development@gmail.com',
    packages=find_packages(),
    install_requires=[
        "pyspark"
    ],
    license='MIT',
    description="Improving the development of Spark applications deployed as "
                "jobs on AWS services like Glue and EMR",
    long_description=__long_description__,
    long_description_content_type="text/markdown",
    url='https://github.com/ThiagoPanini/sparksnake',
    keywords='Cloud, AWS, Python, Spark, pyspark',
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Natural Language :: Portuguese (Brazilian)",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Software Development :: Libraries :: Python Modules"
    ],
    python_requires=">=3.0.0"
)
