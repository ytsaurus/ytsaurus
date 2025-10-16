PY3_PROGRAM()

PY_SRCS(
    __main__.py
)

PEERDIR(
    yt/yt/tools/sqllogictest_generator/lib
    library/python/resource
    library/python/find_root
    contrib/libs/sqlite3
    contrib/python/Jinja2
    contrib/python/sqlglot
)

RESOURCE(
    jinja/ql_sqllogic_ut.cpp.jinja ql_sqllogic_ut.cpp.jinja
)

STYLE_PYTHON()

END()
