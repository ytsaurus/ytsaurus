JUNIT6()

JDK_VERSION(21)

SIZE(SMALL)

DEFAULT_JUNIT_JAVA_SRCS_LAYOUT()

PEERDIR(
    yt/yt/flow/examples/java/wait_click_join/wait_click_join
    yt/java/flow/flow-test-utils

    contrib/java/org/junit/jupiter/junit-jupiter
)

DEPENDENCY_MANAGEMENT(
    contrib/java/org/junit/jupiter/junit-jupiter/6.0.2
    contrib/java/org/junit/jupiter/junit-jupiter-api/6.0.2
)

INCLUDE(${ARCADIA_ROOT}/yt/java/flow/dependency_management.inc)

END()
