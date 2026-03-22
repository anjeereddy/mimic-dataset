from setuptools import setup, find_packages
from pathlib import Path
source_root = Path(".")

with(source_root / "requirements.txt").open(encoding = "utf-8") as f:
    requirements = f.readlines()

setup(
    name="mimic project",
    version="0.1.0",
    install_requires = requirements,
    packages=find_packages(),
    include_package_data = True,
    package_data = {"":["../resources/configs/**/*"]},
    author="Anji",
    description="Mimic patients analysis python project.",
    entry_point={
        "group_1":'run=src.main:main'
    }
)