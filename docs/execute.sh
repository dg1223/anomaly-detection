make clean
rm _transformers.rst
rm modules.rst
sphinx-apidoc -o . ../src/caaswx/spark/_transformers/
make html
