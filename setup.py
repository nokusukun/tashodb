import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="tasho",
    version="0.0.3",
    author="Natsuko H.",
    author_email="nokusukun@yahoo.co.jp",
    description="A performant and lightweight NOSQL Database",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/nokusukun/TashoDB",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)