from setuptools import setup, find_packages

setup(
    name="data2sql",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas>=2.0.0",
        "click>=8.0.0",
        "sqlalchemy>=2.0.0",
        "python-dateutil>=2.8.0",
        "tqdm>=4.65.0",
    ],
    entry_points={
        'console_scripts': [
            'data2sql=data2sql.cli:main',
        ],
    },
) 