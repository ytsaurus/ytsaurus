PY3TEST()

SET(YT_SPLIT_FACTOR 30)

ENV(YT_TEST_FILTER=XLARGE)

INCLUDE(../YaMakeDependsBoilerplate.txt)

REQUIREMENTS(
    cpu:32
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(
        ram:56
    )
ELSE()
    REQUIREMENTS(
        ram:24
    )
ENDIF()

END()
