# Shared third-party version pins for all yt/java/flow modules.

DEPENDENCY_MANAGEMENT(
    # gRPC stack — must move together to a single version.
    contrib/java/io/grpc/grpc-api/1.78.0
    contrib/java/io/grpc/grpc-context/1.78.0
    contrib/java/io/grpc/grpc-stub/1.78.0
    contrib/java/io/grpc/grpc-protobuf/1.78.0
    contrib/java/io/grpc/grpc-protobuf-lite/1.78.0
    contrib/java/io/grpc/grpc-netty-shaded/1.78.0
    contrib/java/io/grpc/grpc-services/1.78.0

    # Protobuf / gRPC transitive companions.
    contrib/java/com/google/api/grpc/proto-google-common-protos/2.63.1
    contrib/java/com/google/protobuf/protobuf-javalite/3.25.8

    # Guava stack — one classifier (-jre) for JDK 21 server code.
    contrib/java/com/google/guava/guava/33.5.0-jre
    contrib/java/com/google/guava/failureaccess/1.0.3
    contrib/java/com/google/j2objc/j2objc-annotations/3.1
    contrib/java/com/google/errorprone/error_prone_annotations/2.44.0
    contrib/java/org/checkerframework/checker-qual/3.42.0
    contrib/java/com/google/code/gson/gson/2.12.1
    contrib/java/com/google/code/findbugs/jsr305/3.0.2

    # Micrometer stack.
    contrib/java/io/micrometer/micrometer-core/1.16.3
    contrib/java/io/micrometer/micrometer-commons/1.16.3
    contrib/java/io/micrometer/micrometer-observation/1.16.3

    # Logging.
    contrib/java/org/slf4j/slf4j-api/2.0.17
    contrib/java/org/apache/logging/log4j/log4j-api/2.25.3
    contrib/java/org/apache/logging/log4j/log4j-core/2.25.3
    contrib/java/org/apache/logging/log4j/log4j-slf4j2-impl/2.25.3

    # Misc runtime.
    contrib/java/org/lz4/lz4-java/1.6.0
    contrib/java/org/jspecify/jspecify/1.0.0
    contrib/java/com/github/ben-manes/caffeine/caffeine/3.2.3
    contrib/java/com/beust/jcommander/1.82
    contrib/java/com/hubspot/jinjava/jinjava/2.8.0
)
