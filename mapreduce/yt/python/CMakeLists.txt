OWNER(
    ignat
    exprmntr
)

LIBRARY()

BUILD_ONLY_IF(LINUX)

PY_SRCS(
    __init__.py
    yt_stuff.py
    yt_version.py
)

CONFIGURE_FILE(
    ${ARCADIA_ROOT}/mapreduce/yt/python/yt_version.py.in
    ${ARCADIA_BUILD_ROOT}/mapreduce/yt/python/yt_version.py
)

# See https://sandbox.yandex-team.ru/resource/RESOURCE_ID/view for details.
IF (YT_PRECISE_17_5)
    SET(YT_VERSION 17_5)
ELSEIF (YT_PRECISE_18_4)
    SET(YT_VERSION 18_4)
ELSEIF (YT_PRECISE_18_5)
    SET(YT_VERSION 18_5)
ELSE () # Latest version
    SET(YT_VERSION 18_5)
ENDIF()

PEERDIR(
    contrib/python/yt
    devtools/swag/lib
)

END()

RECURSE(
    test
)
