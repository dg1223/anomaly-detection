#!/bin/bash
# This shell script deals with making automated changes in documentation on execution after making updates to the transformers
# The steps for automating the documentation are as follows:

# [1] Cleaning the existing support modules
make clean
rm _transformers.rst
rm modules.rst
rm -r Web_Documentation

# [2] Execute the Sphinx-apidoc command for creating the fresh skeleton files for the updated Sphinx docstrings
sphinx-apidoc -o . ../src/caaswx/spark/_transformers/

# [3] Generate the updated documentation
echo "Generating the updated documentation"
make html
echo "Documentation has been updated."

# [6] Rename the _build folder to Documentation
mv _build/html .
mv html Web_Documentation
rm -r _build
firefox Web_Documentation/index.html
