PY3TEST()

SET(YT_SPLIT_FACTOR 40)

ENV(YT_TEST_FILTER=LARGE)

INCLUDE(../YaMakeDependsBoilerplate.txt)

IF (SANITIZER_TYPE)
    REQUIREMENTS(
        cpu:42
        ram:52
    )
ELSE()
    REQUIREMENTS(
        cpu:28
        ram:16
    )
ENDIF()

END()
