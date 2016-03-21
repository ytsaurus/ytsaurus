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
    # See https://sandbox.yandex-team.ru/task/53800665/view for details.
    FROM_SANDBOX(FILE 117513621 OUT yt.tar)
ELSE()
    # See https://sandbox.yandex-team.ru/task/53800796/view for details.
    FROM_SANDBOX(FILE 117513795 OUT yt.tar)
ENDIF()

END()

RECURSE(
    test
)
