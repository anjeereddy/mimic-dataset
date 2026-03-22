from setuptools import setup, find_packages

setup(
    name="mimic project",
    version="0.1.0",
    install_requires=[
        "databricks-sdk",
        "PyYAML==6.0.2",
        "setuptools~=65.5.1",
        "pytest",
        "pytest-cov",
        "databricks-cli",
    ],
    packages=find_packages(),
    include_package_data=True,
    package_data={"": ["../resources/configs/**/*"]},
    author="Anji",
    description="Mimic patients analysis python project.",
    entry_point={
        "group_1": 'run=src.main:main'
    }
)
