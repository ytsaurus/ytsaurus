PY3_PROGRAM()

OWNER(g:yt)

PY_SRCS(
    upload_tutorial_data.py
)

SRCS(
    README.md
)

LICENSE_RESTRICTION_EXCEPTIONS(
    # contrib/python/Faker transitively depends on GPL-licensed contrib/python/text-unidecode.
    # This should be fixed in YT-19849
    contrib/python/text-unidecode
)

PEERDIR(
    yt/python/client
    contrib/python/Faker
)

END()
