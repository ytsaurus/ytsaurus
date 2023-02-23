JAVA_PROGRAM()

JDK_VERSION(11)

OWNER(
    g:yt
    g:yt-java
)

PEERDIR(
    yt/java/annotations
    yt/java/ytsaurus-client

    # YT использует интерфейс slf4j для логгирования.
    # В качестве реализации логгера рекомендуется использовать log4j2,
    # который мы подключаем.
    contrib/java/org/apache/logging/log4j/log4j-slf4j-impl

    contrib/java/javax/persistence/persistence-api/1.0
)

JAVA_SRCS(SRCDIR src/main/java **/*)
JAVA_SRCS(SRCDIR src/main/resources **/*)

DEPENDENCY_MANAGEMENT(
    contrib/java/org/apache/logging/log4j/log4j-slf4j-impl/2.13.1
)

LINT(base)
END()
