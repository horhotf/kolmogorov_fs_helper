from setuptools import setup, find_packages

setup(
    name="kolmogorov_fs_helper",
    version="0.1.0",
    description="A helper library Kolvmogorov (Feast) for filesystem and data streaming .",
    author="Igor Rytsarev",
    author_email="igor.rytsarev@glowbyteconsulting.com",
    packages=find_packages(),
    install_requires=[
        "pyarrow", 
        "psycopg2-binary",
        "fastapi",
        "psycopg2-pool",
        "python-json-logger",
        "python-keycloak",
        "cachetools",
        "slowapi",
        "hdfs",
        "humanfriendly",
        "protobuf==5.29.1",
        "jsonify",
        "pyjwt==2.10.1"
    ],
    python_requires=">=3.9",
)
