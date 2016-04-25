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
    FROM_SANDBOX(FILE 122472590 OUT yt.tar)
ELSEIF (YT_PRECISE_18_2)
    FROM_SANDBOX(FILE 122472776 OUT yt.tar)
ELSEIF (YT_PRECISE_18_3)
    FROM_SANDBOX(FILE 126752449 OUT yt.tar)
ELSE () # Latest version
    FROM_SANDBOX(FILE 126752449 OUT yt.tar)
ENDIF()

PEERDIR(
    devtools/swag
)

END()

RECURSE(
    test
)
