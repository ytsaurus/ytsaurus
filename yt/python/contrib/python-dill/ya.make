PY23_LIBRARY()

PEERDIR(
    library/python/symbols/dill
)

IF (PYTHON2)
    PEERDIR(yt/python/contrib/python-dill/py2)
ELSE()
    PEERDIR(yt/python/contrib/python-dill/py3)
ENDIF()

NO_LINT()

END()

RECURSE(
    py2
    py3
)
