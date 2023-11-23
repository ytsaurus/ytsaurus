JTEST()

IF(OPENSOURCE)
    TAG(ya:not_autocheck)
ENDIF()

JDK_VERSION(11)

IF(OS_LINUX AND NOT OPENSOURCE)
    # Тесты -tt запускаются только под Linux-ом (только там работает YT)
    INCLUDE(${ARCADIA_ROOT}/mapreduce/yt/python/recipe/recipe_with_tablets.inc)
ENDIF()

SIZE(MEDIUM)

DEFAULT_JUNIT_JAVA_SRCS_LAYOUT()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    contrib/java/junit/junit
    contrib/java/org/apache/logging/log4j/log4j-core
    contrib/java/org/apache/logging/log4j/log4j-slf4j-impl
    contrib/java/org/testcontainers/testcontainers
    contrib/java/javax/persistence/persistence-api/1.0
    contrib/java/com/fasterxml/jackson/core/jackson-core
    contrib/java/com/fasterxml/jackson/core/jackson-databind
    contrib/java/org/hamcrest/hamcrest

    yt/java/ytsaurus-client
    yt/java/ytsaurus-testlib
)

# Added automatically to remove dependency on default contrib versions
DEPENDENCY_MANAGEMENT(
    contrib/java/com/fasterxml/jackson/core/jackson-core/2.11.3
    contrib/java/com/fasterxml/jackson/core/jackson-databind/2.11.3
    contrib/java/junit/junit/4.13
    contrib/java/org/apache/logging/log4j/log4j-core/2.13.1
    contrib/java/org/apache/logging/log4j/log4j-slf4j-impl/2.13.1
    contrib/java/org/hamcrest/hamcrest/2.2
    contrib/java/org/testcontainers/testcontainers/1.17.0
)

LINT(extended)
END()
