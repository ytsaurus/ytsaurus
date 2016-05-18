OWNER(
    exprmntr
    vartyukh
)

PACKAGE()

BUILD_ONLY_IF(LINUX)

FILES(
    __init__.py
    yt_stuff.py
)

# See https://sandbox.yandex-team.ru/resource/RESOURCE_ID/view for details.
IF (YT_PRECISE_17_5)
    FROM_SANDBOX(FILE 131697944 OUT yt.tar)
ELSEIF (YT_PRECISE_18_3)
    FROM_SANDBOX(FILE 131698034 OUT yt.tar)
ELSE () # Latest version
    FROM_SANDBOX(FILE 131698034 OUT yt.tar)
ENDIF()

PEERDIR(
    devtools/swag
)

END()

RECURSE(
    test
)
