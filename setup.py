import setuptools

with open("README.md", "r") as f:
    long_description = f.read()

setuptools.setup(
    name='sun_grid_engine_map',
    version="0.0.3",
    author='Sebastian Achim Mueller',
    author_email='sebastian-achim.mueller@mpi-hd.mpg.de',
    description='Map and reduce for qsub.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/cherenkov-plenoscope/sun_grid_engine_map',
    packages=setuptools.find_packages(),
    install_requires=[
        'xmltodict',
        'qstat',
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Unix",
        "Natural Language :: English",
        "Topic :: System :: Distributed Computing",
    ],
    python_requires='>=3',
)
