from setuptools import setup, find_packages

setup(
    name="bronze_framework",
    version="1.0.0",
    description="Metadata-driven bronze layer ingestion framework for Databricks",
    author="Data Engineering Team",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "pyyaml>=6.0",
        "requests>=2.28.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0",
            "pytest-mock>=3.10",
            "chispa>=0.9",
        ],
    },
    python_requires=">=3.9",
)
