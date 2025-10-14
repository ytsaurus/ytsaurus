#include "cache_location.h"

#include "artifact_cache.h"
#include "private.h"

#include <yt/yt/ytlib/chunk_client/format.h>

#include <yt/yt/server/node/data_node/blob_chunk.h>
#include <yt/yt/server/node/data_node/chunk_store.h>
#include <yt/yt/server/node/data_node/config.h>
#include <yt/yt/server/node/data_node/private.h>

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/library/program/program.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NExecNode {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NClusterNode;
using namespace NCypressClient;
using namespace NNode;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TArtifact::TArtifact(
    TCacheLocationPtr location,
    const TChunkDescriptor& descriptor,
    const TArtifactKey& key,
    NChunkClient::TRefCountedChunkMetaPtr meta,
    TClosure destroyedHandler)
    : TAsyncCacheValueBase<TArtifactKey, TArtifact>(key)
    , Id_(descriptor.Id)
    , DestroyedHandler_(std::move(destroyedHandler))
    , FileName_(location->GetChunkPath(descriptor.Id))
    , Location_(location)
    , Meta_(std::move(meta))
    , DiskSpace_(descriptor.DiskSpace)
{ }

TArtifact::~TArtifact()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    DestroyedHandler_.Run();
}

const std::string& TArtifact::GetFileName() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return FileName_;
}

const TCacheLocationPtr& TArtifact::GetLocation() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Location_;
}

i64 TArtifact::GetDiskSpace() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return DiskSpace_;
}

NChunkClient::TChunkId TArtifact::GetId() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Id_;
}

const NChunkClient::TRefCountedChunkMetaPtr& TArtifact::GetMeta() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return Meta_;
}

////////////////////////////////////////////////////////////////////////////////

TCacheLocation::TCacheLocation(
    TString id,
    NDataNode::TCacheLocationConfigPtr config,
    const NClusterNode::IBootstrap* bootstrap,
    TArtifactCachePtr artifactCache)
    : TChunkLocationBase(
        ELocationType::Cache,
        std::move(id),
        config,
        BIND_NO_PROPAGATE(&TCacheLocation::GetBriefConfig, Unretained(this)),
        bootstrap->GetCellId(),
        bootstrap->GetFairShareHierarchicalScheduler(),
        bootstrap->GetHugePageManager(),
        ExecNodeLogger(),
        NDataNode::LocationProfiler())
    , StaticConfig_(config)
    , InThrottler_(CreateNamedReconfigurableThroughputThrottler(
        config->InThrottler,
        "InThrottler",
        Logger,
        Profiler_.WithPrefix("/cache")))
    , ArtifactCache_(std::move(artifactCache))
    , MediumName_(config->MediumName)
    , Bootstrap_(bootstrap)
{
    TChunkLocationBase::UpdateMediumTag(GetMediumName());
}

const NDataNode::TCacheLocationConfigPtr& TCacheLocation::GetStaticConfig() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return StaticConfig_;
}

void TCacheLocation::Reconfigure(NDataNode::TCacheLocationConfigPtr config)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    TChunkLocationBase::Reconfigure(config);
    InThrottler_->Reconfigure(config->InThrottler);
}

IThroughputThrottlerPtr TCacheLocation::GetInThrottler() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

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
        YT_LOG_WARNING("Artifact meta file %v is empty, removing artifact files",
            metaFileName);
    } else if (hasData && !hasMeta) {
        YT_LOG_WARNING(
            "Artifact meta file %v is missing, removing data file %v",
            metaFileName,
            dataFileName);
    } else if (!hasData && hasMeta) {
        YT_LOG_WARNING(
            "Artifact data file %v is missing, removing meta file %v",
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
    YT_ASSERT_INVOKER_AFFINITY(GetAuxPoolInvoker());
    YT_LOG_INFO(
        "Location is disabled; unregistering all the artifacts in it (LocationId: %v)",
        GetId());

    return ArtifactCache_->RemoveArtifactsByLocation(MakeStrong(this));
}

bool TCacheLocation::ScheduleDisable(const TError& reason)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    if (!ChangeState(ELocationState::Disabling, ELocationState::Enabled)) {
        return false;
    }

    YT_LOG_WARNING(reason, "Disabling location (LocationUuid: %v)", GetUuid());

    // No new actions can appear here. Please see TDiskLocation::RegisterAction.
    auto error = TError("Artifact location at %v is disabled", GetPath())
        << TErrorAttribute("location_uuid", GetUuid());
    error = error << reason;
    LocationDisabledAlert_.Store(error);

    YT_UNUSED_FUTURE(BIND([=, this, this_ = MakeStrong(this)] {
        try {
            CreateDisableLockFile(reason);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Creating disable lock file failed");
        }

        try {
            // Fast removal of chunks is necessary to avoid problems with access to chunks on the node.
            WaitFor(RemoveChunks())
                .ThrowOnError();
            WaitFor(BIND(&TCacheLocation::SynchronizeActions, MakeStrong(this))
                .AsyncVia(GetAuxPoolInvoker())
                .Run())
                .ThrowOnError();
            ResetLocationStatistic();
            YT_LOG_INFO("Location disabling finished");
        } catch (const std::exception& ex) {
            YT_LOG_FATAL(ex, "Location disabling error");
        }

        auto finish = ChangeState(ELocationState::Disabled, ELocationState::Disabling);

        if (!finish) {
            YT_LOG_ALERT("Detect location state racing (CurrentState: %v)",
                GetState());
        }
    })
        .AsyncVia(GetAuxPoolInvoker())
        .Run());

    return true;
}

const std::string& TCacheLocation::GetMediumName() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return MediumName_;
}

TBriefChunkLocationConfig TCacheLocation::GetBriefConfig() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    return {
        .AbortOnLocationDisabled = Bootstrap_->GetDynamicConfigManager()->GetConfig()->DataNode->AbortOnLocationDisabled,
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
