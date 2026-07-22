JAVA_PROGRAM(async_request_kotlin_bin)

JDK_VERSION(21)
WITH_JDK()
WITH_KOTLIN()
WITH_KOTLINC_ALLOPEN(preset=spring)

PEERDIR(
    yt/java/flow/flow-runner
    yt/java/flow/flow-spring-boot-starter

    contrib/java/com/fasterxml/jackson/core/jackson-core
    contrib/java/com/fasterxml/jackson/core/jackson-databind
)

DEPENDENCY_MANAGEMENT(
    contrib/java/com/fasterxml/jackson/core/jackson-core/2.19.2
    contrib/java/com/fasterxml/jackson/core/jackson-databind/2.19.2
)

JAVA_SRCS(SRCDIR src/main/kotlin **/*.kt)
JAVA_SRCS(SRCDIR src/main/resources **/*)

LINT(base)

INCLUDE(${ARCADIA_ROOT}/yt/java/flow/dependency_management.inc)

END()
