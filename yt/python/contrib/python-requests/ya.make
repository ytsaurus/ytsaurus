PY23_LIBRARY()

PY_SRCS(
    NAMESPACE yt.packages

    requests/certs.py
    requests/cookies.py
    requests/status_codes.py
    requests/auth.py
    requests/help.py
    requests/adapters.py
    requests/compat.py
    requests/models.py
    requests/api.py
    requests/exceptions.py
    requests/__init__.py
    requests/_internal_utils.py
    requests/sessions.py
    requests/__version__.py
    requests/structures.py
    requests/utils.py
    requests/hooks.py
    requests/packages.py
)

RESOURCE(
    ../../../../certs/cacert.pem /certs/cacert.pem
)

END()
