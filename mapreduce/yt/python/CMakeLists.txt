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
    # See https://sandbox.yandex-team.ru/task/54437636/view for details.
    FROM_SANDBOX(FILE 119356066 OUT yt.tar)
ELSE()
    # See https://sandbox.yandex-team.ru/task/54238013/view for details.
    FROM_SANDBOX(FILE 118800865 OUT yt.tar)
ENDIF()

END()

RECURSE(
    test
)
