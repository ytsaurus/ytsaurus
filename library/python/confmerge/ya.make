PY23_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
  __init__.py
)

PEERDIR(
  contrib/python/deepmerge
)

END()

RECURSE_FOR_TESTS(ut)
