from setuptools import setup, find_packages

with open('README.md', 'r', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="ibex-query-engine",
    version="0.1.0",
    author="Ajna Team",
    author_email="team@ajnacloud.com",
    description="Federated query engine with Apache Iceberg, DuckDB, and multi-source support",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/ibex-query-engine-lib",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.10",
    install_requires=[
        "pydantic>=2.0.0",
        "pyiceberg[s3fs,pyarrow]>=0.7.1",
        "polars>=0.20.31",
        "duckdb>=1.0.0",
        "boto3>=1.34.69",
        "fsspec>=2024.6.1",
        "s3fs>=2024.6.1",
        "pyarrow>=16.1.0",
        "httpx>=0.27.0",
    ],
    extras_require={
        "dev": [
            "pytest>=8.2.0",
            "pytest-asyncio>=0.23.6",
            "black>=24.4.2",
            "ruff>=0.4.8",
        ],
        "lambda": [
            "aws-lambda-typing>=2.18.0",
        ],
        "fastapi": [
            "fastapi>=0.115.0",
            "uvicorn[standard]>=0.30.0",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database",
    ],
)

