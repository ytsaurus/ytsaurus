PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    preset_merger.py
    stages_merger.py
    stages_spec.py
)

PEERDIR(
    library/python/confmerge

    contrib/python/dacite
)

END()

RECURSE_FOR_TESTS(
    ut
)
