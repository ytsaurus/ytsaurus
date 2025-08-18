COBYQA: Constrained Optimization BY Quadratic Approximations
============================================================

.. image:: https://img.shields.io/github/actions/workflow/status/cobyqa/cobyqa/build.yml?logo=github&style=for-the-badge
    :target: https://github.com/cobyqa/cobyqa/actions/workflows/build.yml
.. image:: https://img.shields.io/readthedocs/cobyqa/latest?logo=readthedocs&style=for-the-badge
    :target: https://www.cobyqa.com/
.. image:: https://img.shields.io/codecov/c/github/cobyqa/cobyqa?logo=codecov&style=for-the-badge
    :target: https://codecov.io/gh/cobyqa/cobyqa/
.. image:: https://img.shields.io/pypi/v/cobyqa?logo=pypi&style=for-the-badge
    :target: https://pypi.org/project/cobyqa/
.. image:: https://img.shields.io/pypi/dm/cobyqa?logo=pypi&style=for-the-badge
    :target: https://pypi.org/project/cobyqa/
.. image:: https://img.shields.io/conda/v/conda-forge/cobyqa?logo=anaconda&style=for-the-badge&label=conda-forge
    :target: https://anaconda.org/conda-forge/cobyqa
.. image:: https://img.shields.io/conda/d/conda-forge/cobyqa?logo=anaconda&style=for-the-badge&label=downloads
    :target: https://anaconda.org/conda-forge/cobyqa

COBYQA, an acronym for *Constrained Optimization BY Quadratic Approximations*, is designed to supersede `COBYLA <https://docs.scipy.org/doc/scipy/reference/optimize.minimize-cobyla.html>`_ as a general derivative-free optimization solver.
It can handle unconstrained, bound-constrained, linearly constrained, and nonlinearly constrained problems.
It uses only function values of the objective and constraint functions, if any.
No derivative information is needed.

**Documentation:** https://www.cobyqa.com.

Installation
------------

COBYQA can be installed for `Python 3.8 or above <https://www.python.org>`_.

Dependencies
~~~~~~~~~~~~

The following Python packages are required by COBYQA:

* `NumPy <https://www.numpy.org>`_ 1.17.0 or higher, and
* `SciPy <https://www.scipy.org>`_ 1.10.0 or higher.

If you install COBYQA using ``pip`` or ``conda`` (see below), these dependencies will be installed automatically.


User installation
~~~~~~~~~~~~~~~~~

The easiest way to install COBYQA is using ``pip`` or ``conda``.
To install it using ``pip``, run in a terminal or command window

.. code:: bash

    pip install cobyqa

If you are using ``conda``, you can install COBYQA from the `conda-forge <https://anaconda.org/conda-forge/cobyqa>`_ channel by running

.. code:: bash

    conda install conda-forge::cobyqa

To check your installation, you can execute

.. code:: bash

    python -c "import cobyqa; cobyqa.show_versions()"

If your python launcher is not ``python``, you can replace it with the appropriate command (similarly for ``pip`` and ``conda``).
For example, you may need to use ``python3`` instead of ``python`` and ``pip3`` instead of ``pip``.

Testing
~~~~~~~

To execute the test suite of COBYQA, you first need to install ``pytest``.
You can then run the test suite by executing

.. code:: bash

    pytest --pyargs cobyqa

The test suite takes several minutes to run.
It is unnecessary to run the test suite if you installed COBYQA using the recommended method described above.

Examples
--------

The folder ``examples`` contains a few examples of how to use COBYQA.
These files contain headers explaining what problems they solve.

Support
-------

To report a bug or request a new feature, please open a new issue using the `issue tracker <https://github.com/cobyqa/cobyqa/issues>`_.
