import setuptools
import os

with open("README.rst", "r") as f:
    long_description = f.read()

setuptools.setup(
    name="queue_map_reduce_sebastian-achim-mueller",
    version="1.1.1",
    author="Sebastian Achim Mueller",
    author_email="sebastian-achim.mueller@mpi-hd.mpg.de",
    description="Map and reduce for batch-jobs in distributed computing.",
    long_description=long_description,
    long_description_content_type="text/x-rst",
    url="https://github.com/cherenkov-plenoscope/queue_map_reduce",
    packages=setuptools.find_packages(),
    package_data={
        "queue_map_reduce": [os.path.join("tests", "resources", "*")]
    },
    install_requires=["qstat>=0.0.5",],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Unix",
        "Natural Language :: English",
        "Topic :: System :: Distributed Computing",
        "Topic :: System :: Clustering",
    ],
    python_requires=">=3",
)
