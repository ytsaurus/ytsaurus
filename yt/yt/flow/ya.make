RECURSE(
    bin
    examples
    lib
    library
    tests
    tools
)

IF(NOT OPENSOURCE)
    RECURSE(
        yandex
    )
ENDIF()
