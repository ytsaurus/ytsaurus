JAVA_CONTRIB()

VERSION(1.9)

LICENSE(Apache-2.0)

ORIGINAL_SOURCE(https://repo1.maven.org/maven2)

PEERDIR(
    contrib/java/com/google/code/findbugs/annotations/3.0.1
    contrib/java/com/google/guava/guava/33.1.0-jre
)

EXCLUDE(
    contrib/java/com/google/code/findbugs/jsr305
    contrib/java/net/jcip/jcip-annotations
)

JAR_RESOURCE(7427273329)

SRC_RESOURCE(7427273348)

END()
