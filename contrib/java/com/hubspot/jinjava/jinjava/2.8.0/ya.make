JAVA_CONTRIB()

VERSION(2.8.0)

LICENSE(Apache-2.0)

ORIGINAL_SOURCE(https://repo1.maven.org/maven2)

PEERDIR(
    contrib/java/org/slf4j/slf4j-api/1.7.32
    contrib/java/com/google/guava/guava/33.1.0-jre
    contrib/java/org/javassist/javassist/3.24.1-GA
    contrib/java/com/google/re2j/re2j/1.2
    contrib/java/org/apache/commons/commons-lang3/3.14.0
    contrib/java/commons-net/commons-net/3.9.0
    contrib/java/com/googlecode/java-ipv6/java-ipv6/0.17
    contrib/java/com/google/code/findbugs/annotations/3.0.1
    contrib/java/com/fasterxml/jackson/core/jackson-annotations/2.14.0
    contrib/java/com/fasterxml/jackson/core/jackson-databind/2.14.0
    contrib/java/com/fasterxml/jackson/core/jackson-core/2.14.0
    contrib/java/com/fasterxml/jackson/dataformat/jackson-dataformat-yaml/2.14.0
    contrib/java/com/fasterxml/jackson/datatype/jackson-datatype-jdk8/2.14.0
    contrib/java/ch/obermuhlner/big-math/2.0.0
    contrib/java/com/hubspot/immutables/immutables-exceptions/1.9
)

EXCLUDE(
    contrib/java/com/google/code/findbugs/jsr305
    contrib/java/net/jcip/jcip-annotations
)

JAR_RESOURCE(9455563432)

SRC_RESOURCE(9455562840)

END()
