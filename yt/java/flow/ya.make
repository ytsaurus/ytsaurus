RECURSE(
    flow-core
    flow-runner
    flow-server
    flow-spring-boot-starter
    flow-test-utils
)

IF (NOT OPENSOURCE)
    RECURSE(
        yandex
    )
ENDIF()
