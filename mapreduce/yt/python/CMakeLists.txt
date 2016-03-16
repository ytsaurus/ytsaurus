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

IF (YT_PRECISE_17_5)
    # 0.17.5-prestable-without-yt~7966~df46c24
    FROM_SANDBOX(FILE 116361576 OUT yt.tgz)
ELSE()
    # 18.2.19561-prestable-without-yt~a62ea2a
    FROM_SANDBOX(FILE 116362720 OUT yt.tgz)
ENDIF()

END()

RECURSE(
    test
)
