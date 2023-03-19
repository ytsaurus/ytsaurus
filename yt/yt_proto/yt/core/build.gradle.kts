import com.google.protobuf.gradle.*

val buildProtoDir = File("${buildDir}", "__proto__")

plugins {
    id("java-library")
    id("com.google.protobuf") version "0.8.19"
}

repositories {
    mavenCentral()
}

dependencies {
    api("com.google.protobuf:protobuf-java:3.21.12")

    protobuf(files(File(buildProtoDir, "yt")))
}


val prepareProto = tasks.register<Copy>("prepareProto") {
    from(rootDir) {
        include("yt/yt_proto/yt/core/crypto/proto/crypto.proto")
        include("yt/yt_proto/yt/core/misc/proto/bloom_filter.proto")
        include("yt/yt_proto/yt/core/misc/proto/error.proto")
        include("yt/yt_proto/yt/core/misc/proto/guid.proto")
        include("yt/yt_proto/yt/core/misc/proto/protobuf_helpers.proto")
        include("yt/yt_proto/yt/core/tracing/proto/span.proto")
        include("yt/yt_proto/yt/core/tracing/proto/tracing_ext.proto")
        include("yt/yt_proto/yt/core/bus/proto/bus.proto")
        include("yt/yt_proto/yt/core/rpc/proto/rpc.proto")
        include("yt/yt_proto/yt/core/yson/proto/protobuf_interop.proto")
        include("yt/yt_proto/yt/core/ytree/proto/attributes.proto")
        include("yt/yt_proto/yt/core/ytree/proto/ypath.proto")
    }
    into(buildProtoDir)
}

afterEvaluate {
    tasks.getByName("extractProto").dependsOn(prepareProto)
}
