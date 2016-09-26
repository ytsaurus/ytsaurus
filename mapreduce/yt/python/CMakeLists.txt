OWNER(
    ignat
    exprmntr
)

PACKAGE()

BUILD_ONLY_IF(LINUX)

FILES(
    __init__.py
    yt_stuff.py
)

CONFIGURE_FILE(
    ${ARCADIA_ROOT}/mapreduce/yt/python/yt_version.py.in
    ${ARCADIA_BUILD_ROOT}/mapreduce/yt/python/yt_version.py
)

# See https://sandbox.yandex-team.ru/resource/RESOURCE_ID/view for details.
IF (YT_PRECISE_17_5)
    SET(YT_VERSION 17_5)
    FROM_SANDBOX(FILE 160396240 OUT yt.tar)
ELSEIF (YT_PRECISE_18_4)
    SET(YT_VERSION 18_4)
    FROM_SANDBOX(FILE 155264326 OUT yt.tar)
ELSEIF (YT_PRECISE_18_5)
    SET(YT_VERSION 18_5)
    FROM_SANDBOX(FILE 160396487 OUT yt.tar)
ELSE () # Latest version
    SET(YT_VERSION 18_5)
    FROM_SANDBOX(FILE 160396487 OUT yt.tar)
ENDIF()

IF(NOT YT_PRECISE_17_5)
    PEERDIR(
        yt/${YT_VERSION}/yt/server
        yt/${YT_VERSION}/yt/node_modules
        yt/${YT_VERSION}/yt/nodejs/targets/bin
        yt/${YT_VERSION}/yt/nodejs/targets/package
    )
ENDIF()

PEERDIR(
    devtools/swag
)

END()

RECURSE(
    test
)
