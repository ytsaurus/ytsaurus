JAVA_LIBRARY(flow-test-utils)

DEFAULT_JDK_VERSION(21)

PEERDIR(
    yt/java/flow/flow-core
    yt/java/flow/flow-server
    yt/yt/flow/library/cpp/companion/proto

    contrib/java/org/slf4j/slf4j-api
    contrib/java/com/google/protobuf/protobuf-java

    contrib/java/com/hubspot/jinjava/jinjava

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
