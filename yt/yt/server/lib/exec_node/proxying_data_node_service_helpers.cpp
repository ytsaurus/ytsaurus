#include "proxying_data_node_service_helpers.h"

#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>
#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>

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
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NTableClient;

using namespace NControllerAgent::NProto;

using NObjectClient::TObjectId;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "ProxyingDataNodeService");

////////////////////////////////////////////////////////////////////////////////

TChunkId MakeProxiedChunkId(TChunkId chunkId)
{
    return MakeId(
        EObjectType::Chunk,
        CellTagFromId(chunkId),
        CounterFromId(chunkId),
        EntropyFromId(chunkId));
}

bool CanJobUseProxyingDataNodeService(EJobType jobType)
{
    return jobType != NJobTrackerClient::EJobType::RemoteCopy;
}

void AppendProxiableChunkSpecs(
    const std::vector<TTableSchemaPtr>& schemas,
    const TTableInputSpec& tableSpec,
    EJobType jobType,
    THashMap<TChunkId, TRefCountedChunkSpecPtr>* chunkSpecs)
{
    for (const auto& chunkSpec : tableSpec.chunk_specs()) {
        auto tableIndex = chunkSpec.table_index();

        YT_VERIFY(tableIndex < std::ssize(schemas));

        const auto& schema = schemas[tableIndex];
        auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());

        if (!schema->HasHunkColumns() &&
            IsBlobChunkId(chunkId) &&
            CanJobUseProxyingDataNodeService(jobType))
        {
            auto proxiedChunkId = MakeProxiedChunkId(chunkId);
            auto spec = New<TRefCountedChunkSpec>(chunkSpec);
            spec->set_use_proxying_data_node_service(false);
            chunkSpecs->emplace(
                proxiedChunkId,
                std::move(spec));
        }
    }
}

//! Getting chunks that can be cached in job input cache.
THashMap<TChunkId, TRefCountedChunkSpecPtr> GetProxiableChunkSpecs(
    const TJobSpecExt& jobSpecExt,
    EJobType jobType)
{
    auto dataSourceDirectoryExt = FindProtoExtension<TDataSourceDirectoryExt>(jobSpecExt.extensions());
    std::vector<TTableSchemaPtr> schemas = GetJobInputTableSchemas(
        jobSpecExt,
        dataSourceDirectoryExt ? FromProto<TDataSourceDirectoryPtr>(*dataSourceDirectoryExt) : nullptr);

    THashMap<TChunkId, TRefCountedChunkSpecPtr> chunkSpecs;

    auto scanTableSpecs = [&] (const auto& tableSpecs) {
        for (auto& tableSpec : tableSpecs) {
            AppendProxiableChunkSpecs(schemas, tableSpec, jobType, &chunkSpecs);
        }
    };

    scanTableSpecs(jobSpecExt.input_table_specs());
    scanTableSpecs(jobSpecExt.foreign_input_table_specs());

    return chunkSpecs;
}

void PrepareProxiedChunkReading(
    TNodeId nodeId,
    const THashSet<TChunkId>& hotChunkIds,
    const THashSet<TChunkId>& eligibleChunkIds,
    TTableInputSpec* tableSpec)
{
    // 1. Chunks for which proxying is enabled are added to the new list - proxied_chunk_specs.
    //    For proxying chunks, the nodeId, replicas, and chunkId are replaced.
    // 2. Chunks that are proxied are registered in the job input cache.
    // 3. Inside PatchProxiedChunkSpecs, proxying chunks replace chunks received in
    //    the scheduler and controller spec.
    // 4. JobProxy reads proxied chunks through replication reader via exe node, reads are cached in job input cache.
    // 5. For proxied chunks, the chunkId always describes the EObjectType::Chunk type in order not to use
    //    the erasure reader in the job proxy. Using erasure reader for such chunks is incorrect,
    //    as it leads to incorrect reindexing of blocks.
    std::vector<TChunkSpec> proxiedChunkSpecs;
    proxiedChunkSpecs.reserve(tableSpec->chunk_specs_size());

    for (const auto& chunkSpec : tableSpec->chunk_specs()) {
        auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
        auto proxiedChunkId = MakeProxiedChunkId(chunkId);

        if (!eligibleChunkIds.contains(proxiedChunkId)) {
            continue;
        }

        if (!hotChunkIds.contains(proxiedChunkId) && !chunkSpec.use_proxying_data_node_service()) {
            continue;
        }

        YT_LOG_DEBUG(
            "Modify chunk spec for job input cache (OldChunkId: %v, "
            "NewChunkId: %v, "
            "OldReplicaCount: %v, "
            "NewReplicaCount: %v)",
            chunkId,
            proxiedChunkId,
            chunkSpec.replicas_size(),
            1);

        TChunkSpec proxiedChunkSpec;
        proxiedChunkSpec.CopyFrom(chunkSpec);

        auto newReplicas = TChunkReplicaWithMediumList();
        newReplicas.reserve(1);
        newReplicas.emplace_back(nodeId, 0, AllMediaIndex);

        ToProto(proxiedChunkSpec.mutable_chunk_id(), proxiedChunkId);
        proxiedChunkSpec.set_erasure_codec(ToProto<int>(NErasure::ECodec::None));
        ToProto(proxiedChunkSpec.mutable_replicas(), newReplicas);

        proxiedChunkSpecs.push_back(std::move(proxiedChunkSpec));
    }

    ToProto(tableSpec->mutable_proxied_chunk_specs(), proxiedChunkSpecs);
}

void PrepareProxiedChunkReading(
    TNodeId nodeId,
    const THashSet<TChunkId>& hotChunkIds,
    const THashSet<TChunkId>& eligibleChunkIds,
    TJobSpecExt* jobSpecExt)
{
    auto patchTableSpecs = [&] (auto* tableSpecs) {
        for (auto& tableSpec : *tableSpecs) {
            PrepareProxiedChunkReading(nodeId, hotChunkIds, eligibleChunkIds, &tableSpec);
        }
    };

    patchTableSpecs(jobSpecExt->mutable_input_table_specs());
    patchTableSpecs(jobSpecExt->mutable_foreign_input_table_specs());
}

THashMap<TChunkId, TRefCountedChunkSpecPtr> PatchProxiedChunkSpecs(TJobSpec* jobSpecProto)
{
    THashMap<TChunkId, TRefCountedChunkSpecPtr> chunkIdToOriginalSpec;
    auto* jobSpecExt = jobSpecProto->MutableExtension(TJobSpecExt::job_spec_ext);

    auto patchTableSpec = [&] (auto* tableSpec) {
        THashMap<TChunkId, TChunkSpec> proxiedChunkSpecs;

        for (auto& chunkSpec : tableSpec->proxied_chunk_specs()) {
            proxiedChunkSpecs.emplace(
                FromProto<TChunkId>(chunkSpec.chunk_id()),
                std::move(chunkSpec));
        }

        std::vector<TChunkSpec> newChunkSpecs;
        newChunkSpecs.reserve(tableSpec->chunk_specs_size());

        for (const auto& chunkSpec : tableSpec->chunk_specs()) {
            auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
            auto proxiedChunkId = MakeProxiedChunkId(chunkId);

            auto proxiedChunkSpecIt = proxiedChunkSpecs.find(proxiedChunkId);
            if (proxiedChunkSpecIt.IsEnd()) {
                auto newChunkSpec = chunkSpec;

                // For unpatched chunks, must explicitly set use_proxying_data_node_service = false.
                newChunkSpec.set_use_proxying_data_node_service(false);
                newChunkSpecs.push_back(std::move(newChunkSpec));
                chunkIdToOriginalSpec.emplace(chunkId, New<TRefCountedChunkSpec>(chunkSpec));
            } else {
                const auto& proxiedChunkSpec = proxiedChunkSpecIt->second;
                auto newChunkSpec = chunkSpec;
                newChunkSpec.set_use_proxying_data_node_service(true);
                newChunkSpec.mutable_chunk_id()->CopyFrom(proxiedChunkSpec.chunk_id());
                newChunkSpec.set_erasure_codec(proxiedChunkSpec.erasure_codec());
                newChunkSpec.mutable_replicas()->CopyFrom(proxiedChunkSpec.replicas());
                chunkIdToOriginalSpec.emplace(proxiedChunkId, New<TRefCountedChunkSpec>(chunkSpec));

                YT_LOG_DEBUG(
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

        YT_VERIFY(tableSpec->chunk_specs_size() == std::ssize(newChunkSpecs));

        ToProto(tableSpec->mutable_chunk_specs(), std::move(newChunkSpecs));
    };

    auto patchTableSpecs = [&] (auto* tableSpecs) {
        for (auto& tableSpec : *tableSpecs) {
            patchTableSpec(&tableSpec);
        }
    };

    patchTableSpecs(jobSpecExt->mutable_input_table_specs());
    patchTableSpecs(jobSpecExt->mutable_foreign_input_table_specs());

    return chunkIdToOriginalSpec;
}

void PatchInterruptDescriptor(
    const THashMap<TChunkId, TRefCountedChunkSpecPtr>& chunkIdToOriginalSpec,
    TInterruptDescriptor& interruptDescriptor)
{
    auto restoreOriginalChunkSpecs = [&] (std::vector<TDataSliceDescriptor>& descriptors) {
        for (auto& descriptor : descriptors) {
            for (auto& chunkSpec : descriptor.ChunkSpecs) {
                auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
                if (auto originalSpecIt = chunkIdToOriginalSpec.find(chunkId)) {
                    auto originalSpec = originalSpecIt->second;

                    chunkSpec.mutable_chunk_id()->CopyFrom(originalSpec->chunk_id());
                    chunkSpec.set_erasure_codec(originalSpec->erasure_codec());
                    chunkSpec.mutable_replicas()->CopyFrom(originalSpec->replicas());
                }
            }
        }
    };

    restoreOriginalChunkSpecs(interruptDescriptor.UnreadDataSliceDescriptors);
    restoreOriginalChunkSpecs(interruptDescriptor.ReadDataSliceDescriptors);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
