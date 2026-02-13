import com.google.protobuf.gradle.*

val buildProtoDir = File("${buildDir}", "__proto__")

plugins {
    id("java-library")
    id("com.google.protobuf") version "0.8.19"
}


repositories {
    mavenCentral()
}

java {
    withSourcesJar()
    withJavadocJar()
}

dependencies {
    api("com.google.protobuf:protobuf-java:4.33.0")
    api(project(":yql:essentials:providers:common:proto"))

    protobuf(files(buildProtoDir))
}

protobuf {
    protoc {
        // Download from repositories
        artifact = "com.google.protobuf:protoc:4.33.0"
    }

}

val prepareProto = tasks.register<Copy>("prepareProto") {
    from(rootDir) {
        include("yt/yql/providers/ytflow/integration/proto/yt.proto")
        include("yt/yql/providers/ytflow/integration/proto/solomon.proto")
    }
    into(buildProtoDir)
}

afterEvaluate {
    tasks.getByName("extractProto").dependsOn(prepareProto)
}
