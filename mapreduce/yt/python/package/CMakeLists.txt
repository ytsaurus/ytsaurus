OWNER(snowball)

PACKAGE()

SRCDIR(mapreduce/yt/python)

FILES(
    __init__.py
    yt_stuff.py
)

CONFIGURE_FILE(
    ${ARCADIA_ROOT}/mapreduce/yt/python/yt_version.py.in
    ${ARCADIA_BUILD_ROOT}/mapreduce/yt/python/package/yt_version.py
)

PEERDIR(
    devtools/swag
)

IF (YT_PRECISE_17_5)
    SET(YT_VERSION 17_5)
ELSEIF (YT_PRECISE_18_4)
    SET(YT_VERSION 18_4)
ELSEIF (YT_PRECISE_18_5)
    SET(YT_VERSION 18_5)
ELSE () # Latest version
    SET(YT_VERSION 18_5)
ENDIF()

END()
