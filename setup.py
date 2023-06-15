from setuptools import setup, find_packages
from fugue_cloudprovider_version import __version__


with open("README.md", "r") as f:
    LONG_DESCRIPTION = f.read()

setup(
    name="fugue-cloudprovider",
    version=__version__,
    packages=[x for x in find_packages() if x.startswith("fugue")],
    description="A collection of utils for Fugue to run on cloud providers",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    license="Apache-2.0",
    author="The Fugue Development Team",
    author_email="hello@fugue.ai",
    keywords="fugue aws gcp azure databricks",
    url="http://github.com/fugue-project/fugue-cloudprovider",
    install_requires=[
        "fugue>=0.8.3",
    ],
    extras_require={
        "aws": ["boto3", "fs-s3fs"],
        "gcp": ["fs-gcsfs"],
        "databricks": ["databricks-connect", "databricks-cli"],
        "coiled": ["coiled>=0.2.24", "dask[dataframe,distributed]"],
        "anyscale": ["fugue[ray]>=0.8.3", "anyscale"],
    },
    entry_points={
        "fugue.plugins": [
            "databricks = fugue_databricks.registry[databricks]",
            "coiled = fugue_coiled.registry[coiled]",
            "anyscale = fugue_anyscale.registry[anyscale]",
        ]
    },
    classifiers=[
        # "3 - Alpha", "4 - Beta" or "5 - Production/Stable"
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3 :: Only",
    ],
    python_requires=">=3.7",
)
