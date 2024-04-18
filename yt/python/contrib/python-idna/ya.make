PY23_LIBRARY()

NO_LINT()

IF (PYTHON2)
    PEERDIR(yt/python_py2/contrib/python-idna)
ELSE()
    PY_SRCS(
        NAMESPACE yt.packages

        idna/uts46data.py
        idna/codec.py
        idna/compat.py
        idna/intranges.py
        idna/idnadata.py
        idna/core.py
        idna/package_data.py
        idna/__init__.py
    )
ENDIF()

END()
