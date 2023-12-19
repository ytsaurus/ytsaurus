PY3_LIBRARY()

PY_SRCS(
    NAMESPACE yt.packages

    dill/__diff.py
    dill/__info__.py
    dill/__init__.py
    dill/_dill.py
    dill/_objects.py
    dill/_shims.py
    dill/detect.py
    dill/logger.py
    dill/objtypes.py
    dill/pointers.py
    dill/session.py
    dill/settings.py
    dill/source.py
    dill/temp.py
)

END()
