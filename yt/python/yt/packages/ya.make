PY23_LIBRARY()

NO_CHECK_IMPORTS(
    yt.packages.*
)

PY_SRCS(
    NAMESPACE yt.packages

    __init__.py
    expiringdict.py
    importlib.py
)

PEERDIR(
    contrib/python/cloudpickle
    contrib/python/simplejson
    contrib/python/tqdm
)

END()
