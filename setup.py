from setuptools import setup, find_packages

setup(
    name="FrameMQ",
    version="0.1.0",
    description="This is a library to aid real-time video streaming on the pub/sub model.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Ahmed Twabi",
    author_email="itwabi@gmail.com",
    url="https://github.com/itwabi/frameMQ",
    packages=find_packages(exclude=("tests",)),
    python_requires=">=3.9",
    install_requires=[
        # Add runtime dependencies here, e.g., "numpy>=1.21.0"
    ],
    extras_require={
        "dev": [
            "tox",
            "pytest",
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
