JAVA_LIBRARY(ytsaurus-client)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

IF(JDK_VERSION == "")
    JDK_VERSION(11)
ENDIF()

LINT(extended)

INCLUDE(${ARCADIA_ROOT}/yt/gradle.inc)

PROTO_NAMESPACE(yt)
SET(netty_version 4.1.42.Final)

CHECK_JAVA_DEPS(yes)

PEERDIR(
    contrib/java/io/dropwizard/metrics/metrics-core/3.1.2
    contrib/java/org/slf4j/slf4j-api

    contrib/java/io/netty/netty-buffer
    contrib/java/io/netty/netty-codec
    contrib/java/io/netty/netty-common
    contrib/java/io/netty/netty-handler
    contrib/java/io/netty/netty-transport
    contrib/java/io/netty/netty-transport-native-epoll-linux-x86_64

    contrib/java/org/lz4/lz4-java

    yt/java/annotations
    yt/java/skiff
    yt/java/type-info
    yt/java/ytsaurus-client-core

    yt/yt_proto/yt/client
)

DEPENDENCY_MANAGEMENT(
    contrib/java/io/dropwizard/metrics/metrics-core/3.1.2
    contrib/java/org/slf4j/slf4j-api/1.7.7

    contrib/java/io/netty/netty-buffer/${netty_version}
    contrib/java/io/netty/netty-codec/${netty_version}
    contrib/java/io/netty/netty-common/${netty_version}
    contrib/java/io/netty/netty-handler/${netty_version}
    contrib/java/io/netty/netty-transport/${netty_version}
    contrib/java/io/netty/netty-transport-native-epoll-linux-x86_64/${netty_version}

    contrib/java/org/lz4/lz4-java/1.6.0
)

JAVA_SRCS(SRCDIR src/main/java **/*)
JAVA_SRCS(SRCDIR src/main/resources **/*)

END()

RECURSE_FOR_TESTS(
    src/test
    src/test-integration
)
