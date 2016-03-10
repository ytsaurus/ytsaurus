OWNER(
    exprmntr
    vartyukh
)

UNION()

BUILD_ONLY_IF(LINUX)

FILES(
    __init__.py
    yt_stuff.py
)

FROM_SANDBOX(FILE 112204044 OUT yt.tgz)

END()

RECURSE(
    test
)
