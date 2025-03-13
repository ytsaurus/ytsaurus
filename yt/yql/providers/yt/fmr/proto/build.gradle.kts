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
    api("com.google.protobuf:protobuf-java:3.25.5")
    protobuf(files(buildProtoDir))
}

protobuf {
    protoc {
        // Download from repositories
        artifact = "com.google.protobuf:protoc:3.25.5"
    }

}

val prepareProto = tasks.register<Copy>("prepareProto") {
    from(rootDir) {
        include("yt/yql/providers/yt/fmr/proto/coordinator.proto")
        include("yt/yql/providers/yt/fmr/proto/request_options.proto")
    }
    into(buildProtoDir)
}

afterEvaluate {
    tasks.getByName("extractProto").dependsOn(prepareProto)
}
