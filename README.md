consensus-thesis-code
=====================
[![Build Status](https://travis-ci.org/kc1212/consensus-thesis-code.svg?branch=master)](https://travis-ci.org/kc1212/consensus-thesis-code)

The code for my [thesis](https://github.com/kc1212/consensus-thesis).

Installation
------------
```
pip install -r requirements.txt
```
Using `virtualenv` is recommended.

Running tests
-------------
`pytest` is used to run the tests, for example:
* `pytest` to run all tests
* `pytest tests/test_consensus.py` for a single file
* `pytest tests/test_consensus.py::test_acs` for a single test

Running manually
----------------
* First start the discovery server `python -m src.discovery`.
* Then start at least 4 nodes `python -m src.node PORT N T [FLAGS]`, the port number must be unique and the values `N` and `T` must be the same on all the nodes. 
For example `python -m src.node 12345 4 1 --test acs -v`. For more information, see the help `python -m src.node -h`.

