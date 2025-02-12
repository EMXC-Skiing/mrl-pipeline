"""Setup module for the mrl_pipeline package.

It uses setuptools to handle the packaging and distribution of the mrl_pipeline package.
The setup function is called to specify the package metadata and dependencies.

- name: The name of the package.
- packages: The packages to include, excluding the "mrl_pipeline_tests" package.
- install_requires: The list of dependencies required for the package to work.
- extras_require: Additional dependencies for development, including "dagster-webserver" and "pytest".
"""

from setuptools import find_packages, setup

setup(
    name="mrl_pipeline",
    packages=find_packages(exclude=["mrl_pipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
