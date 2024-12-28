PY3TEST()

IF (SANITIZER_TYPE)
    SET(YT_SPLIT_FACTOR 50)
ELSE()
    SET(YT_SPLIT_FACTOR 40)
ENDIF()

IF (SANITIZER_TYPE)
    SET(YT_TIMEOUT 2400)
ENDIF()

ENV(YT_TEST_FILTER=XLARGE)

INCLUDE(../YaMakeDependsBoilerplate.txt)

IF (SANITIZER_TYPE)
    REQUIREMENTS(
        cpu:52
        ram:74
        ram_disk:12
    )
ELSE()
    REQUIREMENTS(
        cpu:38
        ram:24
    )
ENDIF()

END()
