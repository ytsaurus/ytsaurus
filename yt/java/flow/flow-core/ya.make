JAVA_LIBRARY(flow-core)

DEFAULT_JDK_VERSION(21)

PEERDIR(
    yt/yt/flow/library/cpp/companion/proto

    yt/java/ytsaurus-client

    contrib/java/org/slf4j/slf4j-api

    contrib/java/com/google/protobuf/protobuf-java
    contrib/java/com/github/ben-manes/caffeine/caffeine

    contrib/java/org/jspecify/jspecify

    contrib/java/io/micrometer/micrometer-core
)

DEFAULT_JAVA_SRCS_LAYOUT()

INCLUDE(${ARCADIA_ROOT}/yt/java/flow/dependency_management.inc)

ENABLE(SOURCES_JAR)

LINT(base)

END()

RECURSE_FOR_TESTS(
    src/test
    src/test_proto
)
