import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="aim-library",
    version="0.1.4",
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
    install_requires=[
        "aim-status-grpc",
        "redis==4.5.3",
        "hiredis==2.0.0",
    ],
)
