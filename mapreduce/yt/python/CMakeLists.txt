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

# See https://sandbox.yandex-team.ru/resource/RESOURCE_ID/view for details.
IF (YT_PRECISE_17_5)
    FROM_SANDBOX(FILE 160396240 OUT yt.tar)
ELSEIF (YT_PRECISE_18_4)
    FROM_SANDBOX(FILE 155264326 OUT yt.tar)
ELSEIF (YT_PRECISE_18_5)
    FROM_SANDBOX(FILE 160396487 OUT yt.tar)
ELSE () # Latest version
    FROM_SANDBOX(FILE 160396487 OUT yt.tar)
ENDIF()

PEERDIR(
    devtools/swag
)

END()

RECURSE(
    test
)
