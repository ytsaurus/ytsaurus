PY3TEST()

SET(YT_SPLIT_FACTOR 45)

ENV(YT_TEST_FILTER=SMALL)

INCLUDE(../YaMakeDependsBoilerplate.txt)

IF (SANITIZER_TYPE)
    REQUIREMENTS(
        ram:24
        cpu:18
    )
ELSE()
    REQUIREMENTS(
        ram:12
        cpu:16
    )
ENDIF()

END()

