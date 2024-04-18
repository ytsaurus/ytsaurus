PY23_LIBRARY()

NO_LINT()

IF (PYTHON2)
    PEERDIR(yt/python_py2/contrib/python-fusepy)
ELSE()
    PY_SRCS(
        NAMESPACE yt.packages

        fusell.py
        fuse.py
    )
ENDIF()

END()
