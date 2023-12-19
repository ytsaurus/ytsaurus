PY2_LIBRARY()

PY_SRCS(
    NAMESPACE yt.packages

    dill/__diff.py
    dill/__init__.py
    dill/_dill.py
    dill/_objects.py
    dill/_shims.py
    dill/detect.py
    dill/objtypes.py
    dill/pointers.py
    dill/settings.py
    dill/source.py
    dill/temp.py
)

END()
