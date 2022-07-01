from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

requires = []

setup(
    name='devo-collector',
    version='1.2.0',
    author="Devo Inc.",
    author_email="integrations_factory@devo.com",
    maintainer="Felipe Conde",
    maintainer_email="felipe.conde@devo.com",
    description="Integrations Factory Collector SDK",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    include_package_data=True,
    python_requires='>=3.6, <3.10',
    classifiers=[
        "Programming Language :: Python :: 3"
    ],
    install_requires=requires,
    entry_points={
        'console_scripts': ['devo-collector=agent.main:cli']
    }

)
