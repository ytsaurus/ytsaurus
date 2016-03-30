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
    # See https://sandbox.yandex-team.ru/resource/119849416/view for details.
    FROM_SANDBOX(FILE 119849416 OUT yt.tar)
ELSE()
    # See https://sandbox.yandex-team.ru/resource/119849708/view for details.
    FROM_SANDBOX(FILE 119849708 OUT yt.tar)
ENDIF()

END()

RECURSE(
    test
)
