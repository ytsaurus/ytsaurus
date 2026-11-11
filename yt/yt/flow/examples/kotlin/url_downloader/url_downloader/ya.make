JAVA_PROGRAM(url_downloader_kotlin_bin)

JDK_VERSION(21)
WITH_JDK()
WITH_KOTLIN()
WITH_KOTLINC_ALLOPEN(preset=spring)

PEERDIR(
    yt/java/flow/flow-runner
    yt/java/flow/flow-spring-boot-starter

    contrib/java/javax/persistence/persistence-api/1.0
)

JAVA_SRCS(SRCDIR src/main/kotlin **/*.kt)
JAVA_SRCS(SRCDIR src/main/resources **/*)

LINT(base)

INCLUDE(${ARCADIA_ROOT}/yt/java/flow/dependency_management.inc)

END()
