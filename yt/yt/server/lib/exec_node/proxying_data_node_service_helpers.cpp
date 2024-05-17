#include "proxying_data_node_service_helpers.h"

#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>
#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/chunk_client/public.h>
#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>
#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

namespace NYT::NExecNode {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTableClient;

using namespace NControllerAgent::NProto;

using NObjectClient::TObjectId;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("ProxyingDataNodeService");

////////////////////////////////////////////////////////////////////////////////

TChunkId MakeProxiedChunkId(TChunkId chunkId)
{
    return MakeId(
        EObjectType::Chunk,
        CellTagFromId(chunkId),
        CounterFromId(chunkId),
        HashFromId(chunkId));
}

bool CanJobUseProxyingDataNodeService(EJobType jobType)
{
    return jobType != NJobTrackerClient::EJobType::RemoteCopy;
}

void ModifyChunkSpecReplicas(
    NNodeTrackerClient::TNodeId nodeId,
    EJobType jobType,
    const std::vector<TTableSchemaPtr>& schemas,
    TTableInputSpec* tableSpec,
    THashMap<NChunkClient::TChunkId, TRefCountedChunkSpecPtr>* chunkSpecs)
{
    std::vector<NChunkClient::NProto::TChunkSpec> proxiedChunkSpecs;
    proxiedChunkSpecs.reserve(tableSpec->chunk_specs_size());

    for (auto& chunkSpec : *tableSpec->mutable_chunk_specs()) {
        if (!chunkSpec.use_proxying_data_node_service()) {
            continue;
        }

        auto tableIndex = chunkSpec.table_index();

        YT_VERIFY(std::ssize(schemas) > tableIndex);

        const auto& schema = schemas[tableIndex];
        auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());

        // Skip tables with hunk columns.
        if (schema->HasHunkColumns() ||
            !IsBlobChunkId(chunkId) ||
            !CanJobUseProxyingDataNodeService(jobType))
        {
            chunkSpec.set_use_proxying_data_node_service(false);
            continue;
        }

        // 1. Chunks for which proxying is enabled are added to the new list - proxied_chunk_specs.
        //    For proxying chunks, the hostId, replicas, and chunkId are replaced.
        // 2. Chunks that are proxied are registered in the job input cache.
        // 3. Inside PatchProxiedChunkSpecs, proxying chunks replace chunks received in
        //    the scheduler and controller spec.
        // 4. JobProxy reads proxied chunks through replication reader via exe node, reads are cached in job input cache.
        // 5. For proxied chunks, the ChunkId always describes the EObjectType::Chunk type in order not to use
        //    the erasure reader in the job proxy. Using erasure reader for such chunks is incorrect,
        //    as it leads to incorrect reindexing of blocks.
        auto proxiedChunkId = MakeProxiedChunkId(chunkId);

        YT_LOG_INFO(
            "Modify chunk spec for job input cache (OldChunkId: %v, "
            "NewChunkId: %v, "
            "OldReplicaCount: %v, "
            "NewReplicaCount: %v)",
            chunkId,
            proxiedChunkId,
            chunkSpec.replicas_size(),
            1);

        {
            auto spec = New<TRefCountedChunkSpec>(chunkSpec);
            spec->set_use_proxying_data_node_service(false);
            chunkSpecs->emplace(
                proxiedChunkId,
                std::move(spec));
        }

        {
            NChunkClient::NProto::TChunkSpec proxiedChunkSpec;
            proxiedChunkSpec.CopyFrom(chunkSpec);

            auto newReplicas = TChunkReplicaWithMediumList();
            newReplicas.reserve(1);
            newReplicas.emplace_back(nodeId, 0, AllMediaIndex);

            ToProto(proxiedChunkSpec.mutable_chunk_id(), proxiedChunkId);
            proxiedChunkSpec.set_erasure_codec(ToProto<int>(NErasure::ECodec::None));
            ToProto(proxiedChunkSpec.mutable_replicas(), newReplicas);

            proxiedChunkSpecs.push_back(std::move(proxiedChunkSpec));
        }
    }

    ToProto(tableSpec->mutable_proxied_chunk_specs(), proxiedChunkSpecs);
}

THashMap<NChunkClient::TChunkId, TRefCountedChunkSpecPtr> ModifyChunkSpecForJobInputCache(
    NNodeTrackerClient::TNodeId nodeId,
    EJobType jobType,
    TJobSpecExt* jobSpecExt)
{
    auto dataSourceDirectoryExt = FindProtoExtension<NChunkClient::NProto::TDataSourceDirectoryExt>(jobSpecExt->extensions());
    std::vector<TTableSchemaPtr> schemas = GetJobInputTableSchemas(
        *jobSpecExt,
        dataSourceDirectoryExt ? FromProto<TDataSourceDirectoryPtr>(*dataSourceDirectoryExt) : nullptr);

    THashMap<NChunkClient::TChunkId, TRefCountedChunkSpecPtr> chunkSpecs;

    auto reconfigureTableSpecs = [&] (auto* tableSpecs) {
        for (auto& tableSpec : *tableSpecs) {
            ModifyChunkSpecReplicas(nodeId, jobType, schemas, &tableSpec, &chunkSpecs);
        }
    };

    reconfigureTableSpecs(jobSpecExt->mutable_input_table_specs());
    reconfigureTableSpecs(jobSpecExt->mutable_foreign_input_table_specs());

    return chunkSpecs;
}

void PatchProxiedChunkSpecs(TJobSpec* jobSpecProto)
{
    auto* jobSpecExt = jobSpecProto->MutableExtension(TJobSpecExt::job_spec_ext);

    auto patchTableSpec = [&] (auto* tableSpec) {
        if (tableSpec->proxied_chunk_specs_size() == 0) {
            return;
        }

        THashMap<TChunkId, NChunkClient::NProto::TChunkSpec> proxiedChunkSpecs;

        for (auto& chunkSpec : tableSpec->proxied_chunk_specs()) {
            proxiedChunkSpecs.emplace(
                FromProto<TChunkId>(chunkSpec.chunk_id()),
                std::move(chunkSpec));
        }

        std::vector<NChunkClient::NProto::TChunkSpec> newChunkSpecs;
        newChunkSpecs.reserve(tableSpec->chunk_specs_size());

        for (const auto& chunkSpec : tableSpec->chunk_specs()) {
            auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
            auto proxiedChunkId = MakeProxiedChunkId(chunkId);

            auto proxiedChunkSpecIt = proxiedChunkSpecs.find(proxiedChunkId);
            if (proxiedChunkSpecIt.IsEnd()) {
                newChunkSpecs.push_back(chunkSpec);
            } else {
                const auto& proxiedChunkSpec = proxiedChunkSpecIt->second;
                auto newChunkSpec = chunkSpec;

                newChunkSpec.mutable_chunk_id()->CopyFrom(proxiedChunkSpec.chunk_id());
                newChunkSpec.set_erasure_codec(proxiedChunkSpec.erasure_codec());
                newChunkSpec.mutable_replicas()->CopyFrom(proxiedChunkSpec.replicas());

                YT_LOG_INFO(
                    "Modify chunk spec for job input cache ("
                    "OldChunkId: %v, "
                    "NewChunkId: %v, "
                    "OldReplicaCount: %v, "
                    "NewReplicaCount: %v)",
                    chunkId,
                    proxiedChunkId,
                    chunkSpec.replicas_size(),
                    proxiedChunkSpec.replicas_size());

                newChunkSpecs.push_back(std::move(newChunkSpec));
            }
        }

        ToProto(tableSpec->mutable_chunk_specs(), std::move(newChunkSpecs));
    };

    auto patchTableSpecs = [&] (auto* tableSpecs) {
        for (auto& tableSpec : *tableSpecs) {
            patchTableSpec(&tableSpec);
        }
    };

    patchTableSpecs(jobSpecExt->mutable_input_table_specs());
    patchTableSpecs(jobSpecExt->mutable_foreign_input_table_specs());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
