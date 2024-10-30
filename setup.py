from setuptools import setup, find_packages

setup(
    name="automated_qa",
    version="0.2.3",
    packages=find_packages(),
    install_requires=[
        "requests",  # Add any dependencies your script needs here
        "pandas"
    ],
    entry_points={
        "console_scripts": [
            "qa=automated_qa.main:main", 
        ],
    },
    author="Ausaf Khan",
    author_email="kausaf141@gmail.com",
    description="A CLI to manage datasets",
    url="https://github.com/AusafKhan144/automated_qa",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
