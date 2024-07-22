PY3TEST()

SET(YT_SPLIT_FACTOR 45)

ENV(YT_TEST_FILTER=SMALL)

INCLUDE(../YaMakeDependsBoilerplate.txt)

IF (SANITIZER_TYPE)
    REQUIREMENTS(
        ram:30
        cpu:44
    )
ELSE()
    REQUIREMENTS(
        ram:12
        cpu:16
    )
ENDIF()

END()

