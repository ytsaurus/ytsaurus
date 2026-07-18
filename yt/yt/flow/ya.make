RECURSE(
    bin
    examples
    library
    tests
    tools
)

IF(NOT OPENSOURCE)
    RECURSE(
        yandex
    )
ENDIF()
