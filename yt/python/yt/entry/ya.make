PY23_LIBRARY()

NO_CHECK_IMPORTS(
    __yt_entry_point__
)

IF (PYTHON2)
    PEERDIR(yt/python_py2/yt/entry)
ELSE()
    PY_SRCS(
        TOP_LEVEL

        __yt_entry_point__.py
    )
ENDIF()

END()
