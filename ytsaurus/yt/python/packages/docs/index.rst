.. YT documentation master file, created by
   sphinx-quickstart on Mon Jan  9 15:05:52 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to YTsaurus's documentation!
==============================

Version: |version|.

`YTsaurus <https://ytsaurus.tech>`_ (or YT) is a next generation distributed computation platform
based on Map-Reduce computation paradigm. This page is dedicated to Python library and command-line
tools for the system. Besides API documentation this page also has basic usage example, changelog
and covers configuration.

This documentation does not pretend to be full. Full documentation is available
`online <https://ytsaurus.tech/docs/en/>`_ .

Python library can be divided into two parts:

  1. YT client (also known as Python Wrapper)
  2. YSON library (python implementation of yson parser and writer)

For more info about YSON please visit `YSON docs page <https://ytsaurus.tech/docs/en/user-guide/storage/yson>`_.

If you have any troubles please mail to `dev@ytsaurus.tech` with full description of your
problem and YTsaurus team will try to help you :)

Installation
------------

The simplest way to install the library and CLIs is using pip.

PyPI package
~~~~~~~~~~~~

To install just type the following command in your terminal::

    $ pip install ytsaurus-client


Other ways of installation can be found in documentation.

Usage
-----

Let's make table and write something to it::

    >>> import yt.wrapper as yt
    >>> yt.config["proxy"]["url"] = "<cluster_name>"
    >>> yt.config["token"] = "<your_oauth_token_here>"
    >>> yt.create("table", "//tmp/my_table")
    >>> yt.write_table("//tmp/my_table", [{"x": 1}], format="json")

Lines starting with `yt.config` configure client. First line::

    >>> yt.config["proxy"]["url"] = "<cluster_name>"

sets cluster endpoint url and the next line::

    >>> yt.config["token"] = "<your_oauth_token_here>"

sets token by which user will be authenticated on cluster.
More information about `Authentication <https://ytsaurus.tech/docs/en/user-guide/storage/auth>`_ can be found in docs.

All config options can be found on :ref:`Configuration <configuration>` page.

Other lines are obvious: code creates table `//tmp/my_table` and writes row with key `x` and value `1` to it.

To run map operation library has :func:`run_map <yt.wrapper.run_operation_commands.run_map>` function::

    >>> yt.run_map("cat", "//tmp/my_table", "//tmp/output", format="json")

Output table should contain the same row (since map operation uses `cat` as mapper)::

    >>> assert list(yt.read_table("//tmp/output", format="json")) == [{"x": 1}]

That's it! For more examples see `docs <https://ytsaurus.tech/docs/en/api/python/examples>`_.

Source code
-----------

Library is actively developed on GitHub, code is available `here <https://github.com/ytsaurus/ytsaurus/tree/main/yt/python>`_.

.. toctree::
   :maxdepth: 1
   :hidden:

   yt
   package_changelog
   configuration
