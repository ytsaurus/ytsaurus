#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
requests.certs
~~~~~~~~~~~~~~

This module returns the preferred default CA certificate bundle. There is
only one — the one from the certifi package.

If you are packaging Requests, e.g., for a Linux distribution or a managed
environment, you can change the definition of where() to return a separately
packaged CA bundle.
"""

import os
import sys

def where():
    is_arcadia_python = hasattr(sys, "extra_modules")

    if is_arcadia_python:
        try:
            import library.python.resource
            return library.python.resource.find("/certs/cacert.pem")
        except ImportError:
            # NB: in build with -DOPENSOURCE library.python.resource is unavailable.
            return None
    else:
        return os.path.join(os.path.dirname(__file__), "cacert.pem")


if __name__ == '__main__':
    print(where())
