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
    # See https://sandbox.yandex-team.ru/task/54115644/view for details.
    FROM_SANDBOX(FILE 118450047 OUT yt.tar)
ELSE()
    # See https://sandbox.yandex-team.ru/task/54115725/view for details.
    FROM_SANDBOX(FILE 118450421 OUT yt.tar)
ENDIF()

END()

RECURSE(
    test
)
