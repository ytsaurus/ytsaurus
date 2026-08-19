JAVA_PROGRAM()

JDK_VERSION(21)
WITH_JDK()

PEERDIR(
    yt/java/flow/flow-runner
    yt/java/flow/flow-spring-boot-starter

    contrib/java/com/fasterxml/jackson/core/jackson-core
    contrib/java/com/fasterxml/jackson/core/jackson-databind
)

DEFAULT_JAVA_SRCS_LAYOUT()

DEPENDENCY_MANAGEMENT(
    contrib/java/com/fasterxml/jackson/core/jackson-core/2.19.2
    contrib/java/com/fasterxml/jackson/core/jackson-databind/2.19.2
)

LINT(base)

INCLUDE(${ARCADIA_ROOT}/yt/java/flow/dependency_management.inc)

END()
