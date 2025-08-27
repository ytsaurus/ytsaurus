PY3_LIBRARY()

SUBSCRIBER(g:yt-python)

PY_SRCS(
    account.py
    admin.py
    check_is_paths_exists.py
    helpers.py
    get_attributes.py
    list_dir.py
)

PEERDIR(
    yt/python/client
    contrib/python/pydantic/pydantic-2
)

END()
