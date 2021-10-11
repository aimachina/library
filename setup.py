import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="aim-library",
    version="0.0.11b5",
    author="AIMachina",
    author_email="ticketai@outlook.com",
    description="AIMachina library",
    url="https://github.com/aimachina/library",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
