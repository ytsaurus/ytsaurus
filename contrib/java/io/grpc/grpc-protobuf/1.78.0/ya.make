JAVA_CONTRIB()

VERSION(1.78.0)

LICENSE(Apache-2.0)

ORIGINAL_SOURCE(https://repo1.maven.org/maven2)

PEERDIR(
    contrib/java/io/grpc/grpc-api/1.78.0
    contrib/java/com/google/code/findbugs/jsr305/3.0.2
    contrib/java/com/google/protobuf/protobuf-java/3.25.8
    contrib/java/com/google/api/grpc/proto-google-common-protos/2.63.1
    contrib/java/com/google/guava/guava/33.5.0-android
    contrib/java/io/grpc/grpc-protobuf-lite/1.78.0
)

EXCLUDE(
    contrib/java/com/google/api/api-common
    contrib/java/com/google/protobuf/protobuf-javalite
)

JAR_RESOURCE(10952952727)

SRC_RESOURCE(10952955108)

END()
