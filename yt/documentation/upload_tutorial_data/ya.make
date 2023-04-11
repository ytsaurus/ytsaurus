PY3_PROGRAM()

OWNER(g:yt)

PY_SRCS(
    upload_tutorial_data.py
)

SRCS(
    README.md
)

PEERDIR(
    yt/python/client
    contrib/python/Faker
)

END()
