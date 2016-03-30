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

# See https://sandbox.yandex-team.ru/resource/RESOURCE_ID/view for details.
IF (YT_PRECISE_17_5)
    FROM_SANDBOX(FILE 119849416 OUT yt.tar)
ELSEIF (YT_PRECISE_18_2)
    FROM_SANDBOX(FILE 119849708 OUT yt.tar)
ELSE () # Latest version
    FROM_SANDBOX(FILE 119849708 OUT yt.tar)
ENDIF()

END()

RECURSE(
    test
)
