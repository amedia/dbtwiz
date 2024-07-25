from setuptools import find_packages, setup

setup(
    name="dbtwiz",
    version="0.1.2",
    author="Amedia Produkt og Teknologi",
    url="https://github.com/amedia/dbtwiz",
    license="MIT",
    packages=find_packages(),
    python_requires=">=3.11.0",
    install_requires=[
        "typer>=0.12.0",
        "rich>=13.7.0",
        "dbt-core>=1.7.8,<1.8.0",
        "iterfzf>=1.4.0",
        "google-cloud-storage>=2.14.0",
    ],
    entry_points={
        "console_scripts": [
            "dbtwiz=dbtwiz.main:main",
        ]
    },
)
