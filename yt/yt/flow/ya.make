RECURSE(
    bin
    examples
    extensions
    library
    tests
    tools
)

IF(NOT OPENSOURCE)
    RECURSE(
        yandex
    )
ENDIF()
