JTEST()

IF(OPENSOURCE)
    TAG(ya:not_autocheck)
ENDIF()

JDK_VERSION(11)

ENV(YT_STUFF_MAX_START_RETRIES=10)

IF(OS_LINUX AND NOT OPENSOURCE)
    SET(YT_CONFIG_PATCH {wait_tablet_cell_initialization=%true;node_count=2;node_config={bus_server={bind_retry_count=1}};rpc_proxy_count=1})

    INCLUDE(${ARCADIA_ROOT}/mapreduce/yt/python/recipe/recipe_with_multicells.inc)

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

    yt/java/ytsaurus-client
    yt/java/ytsaurus-testlib
)

# Added automatically to remove dependency on default contrib versions
DEPENDENCY_MANAGEMENT(
    contrib/java/junit/junit/4.13
    contrib/java/org/apache/logging/log4j/log4j-core/2.13.1
    contrib/java/org/apache/logging/log4j/log4j-slf4j-impl/2.13.1
)

LINT(extended)
END()
