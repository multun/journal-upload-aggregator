import os
from setuptools import setup, find_packages


def read(shortname):
    filename = os.path.join(os.path.dirname(__file__), shortname)
    with open(filename, encoding="utf-8") as f:
        contents = f.read()
    return contents


setup(
    name="journal-upload-aggregator",
    version="1.0",
    author="multun",
    author_email="victor.collod@gmail.com",
    scripts=["journal-upload-aggregator"],
    description="An alternative to systemd-journal-remote, inserting logs into elasticsearch.",
    license="MIT",
    keywords="systemd systemd-journald journald elasticsearch",
    url="https://github.com/multun/journal-upload-aggregator/",
    packages=find_packages(),
    python_requires=">= 3.7",
    install_requires=["aiohttp", "async_generator", "backoff", "prometheus_client"],
    long_description=read("README.md"),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Topic :: System :: Logging",
        "Topic :: System :: Monitoring",
        "License :: OSI Approved :: MIT License",
    ],
)
