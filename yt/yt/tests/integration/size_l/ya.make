PY3TEST()

SET(YT_SPLIT_FACTOR 55)

IF (SANITIZER_TYPE)
    SET(YT_TIMEOUT 2000)
ENDIF()

ENV(YT_TEST_FILTER=LARGE)

INCLUDE(../YaMakeDependsBoilerplate.txt)

IF (SANITIZER_TYPE)
    REQUIREMENTS(
        cpu:48
        ram:56
    )
ELSE()
    REQUIREMENTS(
        cpu:28
        ram:20
    )
ENDIF()

END()
