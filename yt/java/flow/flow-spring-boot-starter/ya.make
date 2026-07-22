JAVA_LIBRARY(flow-spring-boot-starter)

DEFAULT_JDK_VERSION(21)

PEERDIR(
    yt/java/flow/flow-core
    yt/java/flow/flow-server

    contrib/java/org/springframework/boot/spring-boot-starter-log4j2
    contrib/java/org/springframework/boot/spring-boot-autoconfigure
    contrib/java/org/springframework/spring-context

    contrib/java/org/jspecify/jspecify
)

DEFAULT_JAVA_SRCS_LAYOUT()

INCLUDE(${ARCADIA_ROOT}/yt/java/flow/dependency_management.inc)

DEPENDENCY_MANAGEMENT(
    contrib/java/org/springframework/boot/spring-boot-starter-log4j2/4.0.2
    contrib/java/org/springframework/boot/spring-boot-autoconfigure/4.0.2
    contrib/java/org/springframework/spring-context/7.0.3
)

EXCLUDE(
    contrib/java/org/springframework/boot/spring-boot-starter-logging
)

ENABLE(SOURCES_JAR)

LINT(base)

END()

RECURSE_FOR_TESTS(
    src/test
)
