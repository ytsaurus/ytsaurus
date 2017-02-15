.. YT documentation master file, created by
   sphinx-quickstart on Mon Jan  9 15:05:52 2017.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to YT's documentation!
==============================

Version: |version|.

`YT <https://wiki.yandex-team.ru/yt/>`_ is a next generation distributed computation platform
based on Map-Reduce computation paradigm. This page is dedicated to Python library and command-line
tools for the system. Besides API documentation this page also has basic usage example, changelog
and covers configuration.

This documentation does not pretend to be full. Full documentation is available on
`wiki <https://wiki.yandex-team.ru/yt/userdoc/pythonwrapper/>`_ page.

Python library can be divided into two parts:

  1. YT client (also known as Python Wrapper)
  2. YSON library (python implementation of yson parser and writer)

For more info about YSON please visit `YSON wiki page <https://wiki.yandex-team.ru/yt/userdoc/yson/>`_.

If you have any troubles please mail to `yt@yandex-team.ru` with full description of your
problem and YT team will help you :)

Installation
------------

There are many ways to install the library and CLIs.

PyPI package
~~~~~~~~~~~~

To install just type the following command in your terminal::

    $ pip install yandex-yt -i https://pypi.yandex-team.ru/simple

Debian package
~~~~~~~~~~~~~~

The following command installs all necessary stuff::

    $ sudo apt-get install yandex-yt-python

If package not found make sure you have `common/unstable` repository in your apt sources::

    deb http://dist.yandex.ru/common unstable/all/

Other ways of installation can be found on wiki page.

Usage
-----

Let's make table and write something to it::

    >>> import yt.wrapper as yt
    >>> yt.config["proxy"]["url"] = "hahn"
    >>> yt.config["token"] = "<your_oauth_token_here>"
    >>> yt.create("table", "//tmp/my_table")
    >>> yt.write_table("//tmp/my_table", [{"x": 1}], format="json")

Lines starting with `yt.config` configure client. First line::

    >>> yt.config["proxy"]["url"] = "hahn"

sets cluster endpoint url (short `hahn` can be used instead of `hahn.yt.yandex.net`) and the next line::

    >>> yt.config["token"] = "<your_oauth_token_here>"

sets OAuth token by which user will be authenticated on cluster.

Special `getting started <https://wiki.yandex-team.ru/yt/gettingstarted/>`_ page answers such questions as
how to obtain token or network access.

All config options can be found on :ref:`Configuration <configuration>` page.

Other lines are obvious: code creates table `//tmp/my_table` and writes row with key `x` and value `1` to it.

To run map operation library has :func:`run_map <yt.wrapper.run_operation_commands.run_map>` function::

    >>> yt.run_map("cat", "//tmp/my_table", "//tmp/output", format="json")

Output table should contain the same row (since map operation uses `cat` as mapper)::

    >>> assert list(yt.read_table("//tmp/output", format="json")) == [{"x": 1}]

That's it! For more examples see `wiki <https://wiki.yandex-team.ru/yt/userdoc/pythonwrapper/>`_.

Source code
-----------

Library is actively developed on GitHub, code is available `here <https://github.yandex-team.ru/yt/python>`_.

.. toctree::
   :maxdepth: 1
   :hidden:

   yt
   package_changelog
   configuration
