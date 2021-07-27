import setuptools

import honey_ryder

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="honey_ryder",
    version=honey_ryder.__version__,
    author="Chris Hannam",
    author_email="ch@chrishannam.co.uk",
    description="Display telemetry data and spot anomalies.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/chrishannam/f1-2021",
    packages=setuptools.find_packages(exclude=("tests", "examples", "data")),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "click",
        "fastavro",
        "influxdb-client",
        "kafka-python",
        "pyserial",
    ],
    include_package_data=True,
    entry_points={
        "console_scripts": [
            "honey_ryder-recorder=honey_ryder.main:run",
        ]
    },
)
