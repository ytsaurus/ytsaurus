"""Shared helpers for the simplejson C extension test files."""
import unittest

from simplejson import encoder


def has_speedups():
    return encoder.c_make_encoder is not None


def skip_if_speedups_missing(func):
    def wrapper(*args, **kwargs):
        if not has_speedups():
            raise unittest.SkipTest("C Extension not available")
        return func(*args, **kwargs)

    return wrapper
