simplejson
----------

simplejson is a simple, fast, complete, correct and extensible
JSON <http://json.org> encoder and decoder for Python 3.8+
with legacy support for Python 2.7.  It is pure Python code
with no dependencies, but includes an optional C extension
for a serious speed boost.

The latest documentation for simplejson can be read online here:
https://simplejson.readthedocs.io/

simplejson is the externally maintained development version of the
json library included with Python (since 2.6). This version is tested
with Python 3.14 (including free-threaded builds) and maintains
backwards compatibility with Python 3.8+. A legacy Python 2.7 wheel
is also published.

The encoder can be specialized to provide serialization in any kind of
situation, without any special support by the objects to be serialized
(somewhat like pickle). This is best done with the ``default`` kwarg
to dumps.

The decoder can handle incoming JSON strings of any specified encoding
(UTF-8 by default). It can also be specialized to post-process JSON
objects with the ``object_hook`` or ``object_pairs_hook`` kwargs. This
is particularly useful for implementing protocols such as JSON-RPC
that have a richer type system than JSON itself.

For those of you that have legacy systems to maintain, there is a
very old fork of simplejson in the `python2.2`_ branch that supports
Python 2.2. This is based on a very old version of simplejson,
is not maintained, and should only be used as a last resort.

.. _python2.2: https://github.com/simplejson/simplejson/tree/python2.2

RawJSON
~~~~~~~

``RawJSON`` allows embedding pre-encoded JSON strings into output without
re-encoding them.

This can be useful in advanced cases where JSON content is already
serialized and re-encoding would be unnecessary.

Example usage::

    from simplejson import dumps, RawJSON

    payload = {
        "status": "ok",
        "data": RawJSON('{"a": 1, "b": 2}')
    }

    print(dumps(payload))
    # Output: {"status": "ok", "data": {"a": 1, "b": 2}}

**Caveat:** ``RawJSON`` should be used with care. It bypasses normal
serialization and validation, and is not recommended for general use
unless the embedded JSON content is fully trusted.
