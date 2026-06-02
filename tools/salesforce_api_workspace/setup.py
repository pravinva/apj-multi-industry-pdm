from setuptools import find_packages, setup


setup(
    name="salesforce_api",
    version="0.0.1",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    install_requires=[
        "databricks-sdk>=0.20.0",
        "requests>=2.31.0",
    ],
    python_requires=">=3.10,<3.13",
)
