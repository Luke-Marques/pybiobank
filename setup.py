from setuptools import setup, find_packages

VERSION = '0.0.1' 
DESCRIPTION = 'Python package to interact with UK Biobank data on CREATE.'
LONG_DESCRIPTION = 'Python package to interact with UK Biobank data, stored on the King\'s College London high performance cluster, CREATE.'

# Setting up
setup(
       # the name must match the folder name 'verysimplemodule'
        name='pybiobank', 
        version=VERSION,
        author='Luke Marques',
        author_email='luke.marques@kcl.ac.uk',
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        packages=find_packages(),
        install_requires=[], # add any additional packages that 
        # needs to be installed along with your package. Eg: 'caer'
        keywords=['python'],
        classifiers= [
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Education",
            "Programming Language :: Python :: 2",
            "Programming Language :: Python :: 3",
            "Operating System :: MacOS :: MacOS X",
            "Operating System :: Microsoft :: Windows",
        ],
        zip_safe=False
)