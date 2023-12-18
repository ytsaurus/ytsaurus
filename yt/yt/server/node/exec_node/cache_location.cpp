#include "cache_location.h"

#include "chunk_cache.h"
#include "private.h"

#include <yt/yt/ytlib/chunk_client/format.h>

#include <yt/yt/server/node/data_node/blob_chunk.h>
#include <yt/yt/server/node/data_node/chunk_store.h>
#include <yt/yt/server/node/data_node/config.h>
#include <yt/yt/server/node/data_node/private.h>

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/library/program/program.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NExecNode {

using namespace NChunkClient;
using namespace NClusterNode;
using namespace NCypressClient;
using namespace NDataNode;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TCachedBlobChunk::TCachedBlobChunk(
    TChunkContextPtr context,
    TChunkLocationPtr location,
    const TChunkDescriptor& descriptor,
    TRefCountedChunkMetaPtr meta,
    const TArtifactKey& key,
    TClosure destroyedHandler)
    : TBlobChunkBase(
        std::move(context),
        std::move(location),
        descriptor,
        std::move(meta))
    , TAsyncCacheValueBase<TArtifactKey, TCachedBlobChunk>(key)
    , DestroyedHandler_(std::move(destroyedHandler))
{ }

TCachedBlobChunk::~TCachedBlobChunk()
{
    VERIFY_THREAD_AFFINITY_ANY();

    DestroyedHandler_.Run();
}

////////////////////////////////////////////////////////////////////////////////

TCacheLocation::TCacheLocation(
    TString id,
    TCacheLocationConfigPtr config,
    TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
    TChunkContextPtr chunkContext,
    IChunkStoreHostPtr chunkStoreHost,
    TChunkCachePtr chunkCache)
    : TChunkLocation(
        ELocationType::Cache,
        std::move(id),
        config,
        std::move(dynamicConfigManager),
        nullptr,
        std::move(chunkContext),
        std::move(chunkStoreHost))
    , InThrottler_(CreateNamedReconfigurableThroughputThrottler(
        config->InThrottler,
        "InThrottler",
        Logger,
        Profiler_.WithPrefix("/cache")))
    , ChunkCache_(std::move(chunkCache))
{ }

const IThroughputThrottlerPtr& TCacheLocation::GetInThrottler() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return InThrottler_;
}

std::optional<TChunkDescriptor> TCacheLocation::Repair(
    TChunkId chunkId,
    const TString& metaSuffix)
{
    auto fileName = GetChunkPath(chunkId);

    auto dataFileName = fileName;
    auto metaFileName = fileName + metaSuffix;

    bool hasData = NFS::Exists(dataFileName);
    bool hasMeta = NFS::Exists(metaFileName);

    if (hasMeta && hasData) {
        i64 dataSize = NFS::GetPathStatistics(dataFileName).Size;
        i64 metaSize = NFS::GetPathStatistics(metaFileName).Size;
        if (metaSize > 0) {
            TChunkDescriptor descriptor;
            descriptor.Id = chunkId;
            descriptor.DiskSpace = dataSize + metaSize;
            return descriptor;
        }
        YT_LOG_WARNING("Chunk meta file %v is empty, removing chunk files",
            metaFileName);
    } else if (hasData && !hasMeta) {
        YT_LOG_WARNING("Chunk meta file %v is missing, removing data file %v",
            metaFileName,
            dataFileName);
    } else if (!hasData && hasMeta) {
        YT_LOG_WARNING("Chunk data file %v is missing, removing meta file %v",
            dataFileName,
            metaFileName);
    }

    if (hasData) {
        NFS::Remove(dataFileName);
    }
    if (hasMeta) {
        NFS::Remove(metaFileName);
    }

    return {};
}

std::optional<TChunkDescriptor> TCacheLocation::RepairChunk(TChunkId chunkId)
{
    std::optional<TChunkDescriptor> optionalDescriptor;
    auto chunkType = TypeFromId(DecodeChunkId(chunkId).Id);
    switch (chunkType) {
        case EObjectType::Chunk:
            optionalDescriptor = Repair(chunkId, ChunkMetaSuffix);
            break;

        case EObjectType::Artifact:
            optionalDescriptor = Repair(chunkId, ArtifactMetaSuffix);
            break;

        default:
            YT_LOG_WARNING("Invalid chunk type (Type: %v, ChunkId: %v)",
                chunkType,
                chunkId);
            break;
    }
    return optionalDescriptor;
}

std::vector<TString> TCacheLocation::GetChunkPartNames(TChunkId chunkId) const
{
    auto primaryName = ToString(chunkId);
    switch (TypeFromId(DecodeChunkId(chunkId).Id)) {
        case EObjectType::Chunk:
            return {
                primaryName,
                primaryName + ChunkMetaSuffix
            };

        case EObjectType::Artifact:
            return {
                primaryName,
                primaryName + ArtifactMetaSuffix
            };

        default:
            YT_ABORT();
    }
}

TFuture<void> TCacheLocation::RemoveChunks()
{
    VERIFY_INVOKER_AFFINITY(GetAuxPoolInvoker());
    YT_LOG_INFO("Location is disabled; unregistering all the chunks in it (LocationId: %v)",
        GetId());

    return ChunkCache_->RemoveChunksByLocation(MakeStrong(this));
}

bool TCacheLocation::ScheduleDisable(const TError& reason)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (!ChangeState(ELocationState::Disabling, ELocationState::Enabled)) {
        return false;
    }

    YT_LOG_WARNING(reason, "Disabling location (LocationUuid: %v)", GetUuid());

    // No new actions can appear here. Please see TDiskLocation::RegisterAction.
    auto error = TError("Chunk location at %v is disabled", GetPath())
        << TErrorAttribute("location_uuid", GetUuid());
    error = error << reason;
    LocationDisabledAlert_.Store(error);

    bool finish = false;

    try {
        WaitFor(BIND([=, this, this_ = MakeStrong(this)] () {
            WaitFor(SynchronizeActions())
                .ThrowOnError();
            WaitFor(RemoveChunks())
                .ThrowOnError();
            ResetLocationStatistic();
            CreateDisableLockFile(reason);
        })
            .AsyncVia(GetAuxPoolInvoker())
            .Run())
            .ThrowOnError();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Location disabling error");
    }

    finish = ChangeState(ELocationState::Disabled, ELocationState::Disabling);

    if (!finish) {
        YT_LOG_ALERT("Detect location state racing (CurrentState: %v)",
            GetState());
    }

    return finish;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
