PY3TEST()

SET(YT_SPLIT_FACTOR 40)

ENV(YT_TEST_FILTER=LARGE)

INCLUDE(../YaMakeDependsBoilerplate.txt)

REQUIREMENTS(
    cpu:24
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(
        ram:40
    )
ELSE()
    REQUIREMENTS(
        ram:16
    )
ENDIF()

END()
