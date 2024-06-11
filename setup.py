#!/usr/bin/env python3

import sys
import json
from setuptools import find_packages, setup

if sys.version_info < (3, 12):
    raise ValueError("Requires Python 3.12+")


def requires_from_pipfile_lock(filename: str) -> list:
    with open(filename, "r", encoding="utf-8") as f:
        pipfile_lock = json.load(f)

    default_deps = pipfile_lock.get("default", {})

    return [f"{pkg}{info['version']}" for pkg, info in default_deps.items()]


with open("README.md", "r", encoding="utf-8") as f:
    readme = f.read()

setup(
    name="dbt-metabase",
    description="dbt + Metabase integration. auto-creating dashbords from dbt models and exposes",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="Mike Gouline, Alexander Korbashov",
    url="https://github.com/korbash/dbt-metabase",
    license="MIT License",
    entry_points={
        "console_scripts": ["dbt-metabase = dbtmetabase.__main__:cli"],
    },
    packages=find_packages(exclude=["tests", "sandbox"]),
    install_requires=requires_from_pipfile_lock("Pipfile.lock"),
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
