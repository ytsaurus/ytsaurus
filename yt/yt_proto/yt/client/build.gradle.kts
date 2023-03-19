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
    api(project(":yt:yt_proto:yt:core"))

    protobuf(files(File(buildProtoDir, "yt")))
}


val prepareProto = tasks.register<Copy>("prepareProto") {
    from(rootDir) {
        include("yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.proto")
        include("yt/yt_proto/yt/client/api/rpc_proxy/proto/discovery_service.proto")
        include("yt/yt_proto/yt/client/cell_master/proto/cell_directory.proto")
        include("yt/yt_proto/yt/client/chaos_client/proto/replication_card.proto")
        include("yt/yt_proto/yt/client/chunk_client/proto/data_statistics.proto")
        include("yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.proto")
        include("yt/yt_proto/yt/client/chunk_client/proto/read_limit.proto")
        include("yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.proto")
        include("yt/yt_proto/yt/client/chunk_client/proto/confirm_chunk_replica_info.proto")
        include("yt/yt_proto/yt/client/hive/proto/timestamp_map.proto")
        include("yt/yt_proto/yt/client/hive/proto/cluster_directory.proto")
        include("yt/yt_proto/yt/client/node_tracker_client/proto/node.proto")
        include("yt/yt_proto/yt/client/node_tracker_client/proto/node_directory.proto")
        include("yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.proto")
        include("yt/yt_proto/yt/client/table_chunk_format/proto/column_meta.proto")
        include("yt/yt_proto/yt/client/table_chunk_format/proto/wire_protocol.proto")
        include("yt/yt_proto/yt/client/transaction_client/proto/timestamp_service.proto")
        include("yt/yt_proto/yt/client/query_client/proto/query_statistics.proto")
        include("yt/yt_proto/yt/client/misc/proto/workload.proto")
    }
    into(buildProtoDir)
}

afterEvaluate {
    tasks.getByName("extractProto").dependsOn(prepareProto)
}
