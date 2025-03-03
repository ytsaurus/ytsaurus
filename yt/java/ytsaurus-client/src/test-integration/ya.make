JTEST()

IF(OPENSOURCE)
    TAG(ya:not_autocheck)
ENDIF()

JDK_VERSION(11)

IF(OS_LINUX AND NOT OPENSOURCE)
    SET(YT_CONFIG_PATCH {wait_tablet_cell_initialization=%true;node_count=2;node_config={bus_server={bind_retry_count=1}};rpc_proxy_count=1;rpc_proxy_config={enable_shuffle_service=%true}})

    INCLUDE(${ARCADIA_ROOT}/yt/recipe/basic/recipe.inc)

    REQUIREMENTS(
        ram_disk:4
    )
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
