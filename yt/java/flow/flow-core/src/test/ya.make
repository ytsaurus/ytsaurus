JUNIT6()

JDK_VERSION(21)

SIZE(SMALL)

DEFAULT_JUNIT_JAVA_SRCS_LAYOUT()

PEERDIR(
    yt/java/flow/flow-core
    yt/java/flow/flow-core/src/test_proto
    contrib/java/org/junit/jupiter/junit-jupiter
    contrib/java/org/mockito/mockito-core
    contrib/java/org/apache/logging/log4j/log4j-slf4j2-impl

    contrib/java/javax/persistence/persistence-api/1.0
)

DEPENDENCY_MANAGEMENT(
    contrib/java/org/junit/jupiter/junit-jupiter/6.0.2
    contrib/java/org/junit/jupiter/junit-jupiter-api/6.0.2
    contrib/java/org/mockito/mockito-core/5.21.0
    contrib/java/org/apache/logging/log4j/log4j-slf4j2-impl/2.25.1
)

END()
