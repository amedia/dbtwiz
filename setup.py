from setuptools import find_packages, setup

setup(
    name="dbtwiz",
    version="0.2.0",
    author="Amedia Produkt og Teknologi",
    url="https://github.com/amedia/dbtwiz",
    license="MIT",
    packages=find_packages(),
    python_requires=">=3.11.0",
    install_requires=[
        "typer>=0.15.0",
        "rich>=13.9.0",
        "dbt-core>=1.9.1",
        "iterfzf>=1.4.0",
        "google-cloud-storage>=2.19.0",
    ],
    entry_points={
        "console_scripts": [
            "dbtwiz=dbtwiz.main:main",
        ]
    },
    include_package_data=True,
)
