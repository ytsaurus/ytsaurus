PY23_LIBRARY()

IF (PYTHON2)
    IF (NOT OPENSOURCE)
        PEERDIR(yt/python/contrib/python-dill/py2)
    ENDIF()
ELSE()
    PEERDIR(yt/python/contrib/python-dill/py3)
ENDIF()

NO_LINT()

END()

IF (NOT OPENSOURCE)
    RECURSE(
        py2
    )
ENDIF()

RECURSE(
    py3
)
