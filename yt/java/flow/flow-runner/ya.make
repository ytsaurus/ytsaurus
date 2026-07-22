JAVA_LIBRARY(flow-runner)

DEFAULT_JDK_VERSION(21)

PEERDIR(
    yt/java/flow/flow-core

    contrib/java/org/slf4j/slf4j-api

    contrib/java/com/beust/jcommander

    contrib/java/com/google/protobuf/protobuf-java

    contrib/java/org/jspecify/jspecify
)

DEFAULT_JAVA_SRCS_LAYOUT()

INCLUDE(${ARCADIA_ROOT}/yt/java/flow/dependency_management.inc)

ENABLE(SOURCES_JAR)

LINT(base)

END()

RECURSE_FOR_TESTS(
    src/test
)
