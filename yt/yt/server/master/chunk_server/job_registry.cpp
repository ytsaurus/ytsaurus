#include "job_registry.h"

#include "chunk_manager.h"
#include "config.h"
#include "job.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/master/node_tracker_server/data_center.h>
#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_tracker.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NChunkServer {

using namespace NNodeTrackerClient::NProto;
using namespace NNodeTrackerServer;
using namespace NCellMaster;
using namespace NConcurrency;
using namespace NProfiling;
using namespace NCypressClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

TJobRegistry::TJobRegistry(
    TChunkManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Config_(config)
    , Bootstrap_(bootstrap)
    , JobThrottler_(CreateReconfigurableThroughputThrottler(
        New<TThroughputThrottlerConfig>(),
        ChunkServerLogger,
        ChunkServerProfilerRegistry.WithPrefix("/job_throttler")))
{
    InitInterDCEdges();
    UpdateAllDataCentersSet();
}

TJobRegistry::~TJobRegistry() = default;

void TJobRegistry::Start()
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(DynamicConfigChangedCallback_);
}

void TJobRegistry::Stop()
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->UnsubscribeConfigChanged(DynamicConfigChangedCallback_);
}

void TJobRegistry::OnNodeDataCenterChanged(TNode* node, TDataCenter* oldDataCenter)
{
    YT_ASSERT(node->GetDataCenter() != oldDataCenter);

    for (const auto& [jobId, job] : node->IdToJob()) {
        UpdateInterDCEdgeConsumption(job, oldDataCenter, -1);
        UpdateInterDCEdgeConsumption(job, node->GetDataCenter(), +1);
    }
}

int TJobRegistry::GetCappedSecondaryCellCount()
{
    return std::max<int>(1, Bootstrap_->GetMulticellManager()->GetSecondaryCellTags().size());
}

void TJobRegistry::InitInterDCEdges()
{
    UpdateInterDCEdgeCapacities();
    InitUnsaturatedInterDCEdges();
}

void TJobRegistry::InitUnsaturatedInterDCEdges()
{
    UnsaturatedInterDCEdges_.clear();

    const auto defaultCapacity = GetDynamicConfig()->InterDCLimits->GetDefaultCapacity() / GetCappedSecondaryCellCount();

    const auto& nodeTracker = Bootstrap_->GetNodeTracker();

    auto updateForSrcDC = [&] (const TDataCenter* srcDataCenter) {
        auto& interDCEdgeConsumption = InterDCEdgeConsumption_[srcDataCenter];
        const auto& interDCEdgeCapacities = InterDCEdgeCapacities_[srcDataCenter];

        auto updateForDstDC = [&] (const TDataCenter* dstDataCenter) {
            if (interDCEdgeConsumption.Value(dstDataCenter, 0) <
                interDCEdgeCapacities.Value(dstDataCenter, defaultCapacity))
            {
                UnsaturatedInterDCEdges_[srcDataCenter].insert(dstDataCenter);
            }
        };

        updateForDstDC(nullptr);
        for (auto [dataCenterId, dataCenter] : nodeTracker->DataCenters()) {
            if (IsObjectAlive(dataCenter)) {
                updateForDstDC(dataCenter);
            }
        }
    };

    updateForSrcDC(nullptr);
    for (auto [dataCenterId, dataCenter] : nodeTracker->DataCenters()) {
        if (IsObjectAlive(dataCenter)) {
            updateForSrcDC(dataCenter);
        }
    }
}

void TJobRegistry::UpdateInterDCEdgeConsumption(
    const TJobPtr& job,
    const TDataCenter* srcDataCenter,
    int sizeMultiplier)
{
    if (job->GetType() != EJobType::ReplicateChunk &&
        job->GetType() != EJobType::RepairChunk &&
        job->GetType() != EJobType::MergeChunks)
    {
        return;
    }

    auto& interDCEdgeConsumption = InterDCEdgeConsumption_[srcDataCenter];
    const auto& interDCEdgeCapacities = InterDCEdgeCapacities_[srcDataCenter];

    const auto defaultCapacity = GetDynamicConfig()->InterDCLimits->GetDefaultCapacity() / GetCappedSecondaryCellCount();

    auto getReplicas = [&] (const TJobPtr& job) {
        switch (job->GetType()) {
            case EJobType::ReplicateChunk: {
                auto replicateJob = StaticPointerCast<TReplicationJob>(job);
                return replicateJob->TargetReplicas();
            }
            case EJobType::RepairChunk: {
                auto repairJob = StaticPointerCast<TRepairJob>(job);
                return repairJob->TargetReplicas();
            }
            case EJobType::MergeChunks: {
                auto mergeJob = StaticPointerCast<TMergeJob>(job);
                return mergeJob->TargetReplicas();
            }
            default:
                YT_ABORT();
        }
    };

    i64 chunkPartSize = 0;
    switch (job->GetType()) {
        case EJobType::ReplicateChunk:
            chunkPartSize = job->ResourceUsage().replication_data_size();
            break;
        case EJobType::RepairChunk:
            chunkPartSize = job->ResourceUsage().repair_data_size();
            break;
        case EJobType::MergeChunks:
            chunkPartSize = job->ResourceUsage().merge_data_size();
            break;
        default:
            YT_ABORT();
    }

    for (const auto& nodePtrWithIndexes : getReplicas(job)) {
        const auto* dstDataCenter = nodePtrWithIndexes.GetPtr()->GetDataCenter();

        auto& consumption = interDCEdgeConsumption[dstDataCenter];
        consumption += sizeMultiplier * chunkPartSize;

        if (consumption < interDCEdgeCapacities.Value(dstDataCenter, defaultCapacity)) {
            UnsaturatedInterDCEdges_[srcDataCenter].insert(dstDataCenter);
        } else {
            auto it = UnsaturatedInterDCEdges_.find(srcDataCenter);
            if (it != UnsaturatedInterDCEdges_.end()) {
                it->second.erase(dstDataCenter);
                // Don't do UnsaturatedInterDCEdges_.erase(it) here - the memory
                // saving is negligible, but the slowdown may be noticeable. Plus,
                // the removal is very likely to be undone by a soon-to-follow insertion.
            }
        }
    }
}

void TJobRegistry::UpdateAllDataCentersSet()
{
    AllDataCenters_.clear();

    const auto& nodeTracker = Bootstrap_->GetNodeTracker();
    for (auto [dataCenterId, dataCenter] : nodeTracker->DataCenters()) {
        if (IsObjectAlive(dataCenter)) {
            YT_VERIFY(AllDataCenters_.insert(dataCenter).second);
        }
    }
    AllDataCenters_.insert(nullptr);
}

bool TJobRegistry::HasUnsaturatedInterDCEdgeStartingFrom(const TDataCenter* srcDataCenter) const
{
    if (IgnoreEdgeCapacities_) {
        return true;
    }

    auto it = UnsaturatedInterDCEdges_.find(srcDataCenter);
    if (it == UnsaturatedInterDCEdges_.end()) {
        return false;
    }
    return !it->second.empty();
}

void TJobRegistry::OnDataCenterCreated(const TDataCenter* dataCenter)
{
    UpdateInterDCEdgeCapacities();
    UpdateAllDataCentersSet();

    const auto defaultCapacity = GetDynamicConfig()->InterDCLimits->GetDefaultCapacity() / GetCappedSecondaryCellCount();

    auto updateEdge = [&] (const TDataCenter* srcDataCenter, const TDataCenter* dstDataCenter) {
        if (InterDCEdgeConsumption_[srcDataCenter].Value(dstDataCenter, 0) <
            InterDCEdgeCapacities_[srcDataCenter].Value(dstDataCenter, defaultCapacity))
        {
            UnsaturatedInterDCEdges_[srcDataCenter].insert(dstDataCenter);
        }
    };

    updateEdge(nullptr, dataCenter);
    updateEdge(dataCenter, nullptr);

    const auto& nodeTracker = Bootstrap_->GetNodeTracker();
    for (const auto& [dstDataCenterId, otherDataCenter] : nodeTracker->DataCenters()) {
        updateEdge(dataCenter, otherDataCenter);
        updateEdge(otherDataCenter, dataCenter);
    }
}

void TJobRegistry::OnDataCenterDestroyed(const TDataCenter* dataCenter)
{
    UpdateAllDataCentersSet();

    InterDCEdgeCapacities_.erase(dataCenter);
    for (auto& [srcDataCenter, dstDataCenterCapacities] : InterDCEdgeCapacities_) {
        dstDataCenterCapacities.erase(dataCenter); // may be no-op
    }

    InterDCEdgeConsumption_.erase(dataCenter);
    for (auto& [srcDataCenter, dstDataCenterConsumption] : InterDCEdgeConsumption_) {
        dstDataCenterConsumption.erase(dataCenter); // may be no-op
    }

    UnsaturatedInterDCEdges_.erase(dataCenter);
    for (auto& [srcDataCenter, dstDataCenterSet] : UnsaturatedInterDCEdges_) {
        dstDataCenterSet.erase(dataCenter); // may be no-op
    }
}

void TJobRegistry::UpdateInterDCEdgeCapacities()
{
    InterDCEdgeCapacities_.clear();

    auto capacities = GetDynamicConfig()->InterDCLimits->GetCapacities();

    const auto& nodeTracker = Bootstrap_->GetNodeTracker();

    auto updateForSrcDC = [&] (const TDataCenter* srcDataCenter) {
        const std::optional<TString>& srcDataCenterName = srcDataCenter
            ? std::optional<TString>(srcDataCenter->GetName())
            : std::nullopt;
        auto& interDCEdgeCapacities = InterDCEdgeCapacities_[srcDataCenter];
        const auto& newInterDCEdgeCapacities = capacities[srcDataCenterName];

        auto updateForDstDC = [&] (const TDataCenter* dstDataCenter) {
            const std::optional<TString>& dstDataCenterName = dstDataCenter
                ? std::optional<TString>(dstDataCenter->GetName())
                : std::nullopt;
            auto it = newInterDCEdgeCapacities.find(dstDataCenterName);
            if (it != newInterDCEdgeCapacities.end()) {
                interDCEdgeCapacities[dstDataCenter] = it->second / GetCappedSecondaryCellCount();
            }
        };

        updateForDstDC(nullptr);
        for (const auto& pair : nodeTracker->DataCenters()) {
            if (IsObjectAlive(pair.second)) {
                updateForDstDC(pair.second);
            }
        }
    };

    updateForSrcDC(nullptr);
    for (const auto& pair : nodeTracker->DataCenters()) {
        if (IsObjectAlive(pair.second)) {
            updateForSrcDC(pair.second);
        }
    }

    InterDCEdgeCapacitiesLastUpdateTime_ = GetCpuInstant();
}

const TJobRegistry::TDataCenterSet& TJobRegistry::GetUnsaturatedInterDCEdgesStartingFrom(const TDataCenter* dc)
{
    if (IgnoreEdgeCapacities_) {
        return AllDataCenters_;
    }

    return UnsaturatedInterDCEdges_[dc];
}

void TJobRegistry::RegisterJob(const TJobPtr& job)
{
    job->GetNode()->RegisterJob(job);

    auto jobType = job->GetType();
    ++RunningJobs_[jobType];
    ++JobsStarted_[jobType];

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto chunkId = job->GetChunkIdWithIndexes().Id;
    auto* chunk = chunkManager->FindChunk(chunkId);
    if (chunk) {
        chunk->AddJob(job);
    }

    UpdateInterDCEdgeConsumption(job, job->GetNode()->GetDataCenter(), +1);

    JobThrottler_->Acquire(1);

    YT_LOG_DEBUG("Job registered (JobId: %v, JobType: %v, Address: %v)",
        job->GetJobId(),
        job->GetType(),
        job->GetNode()->GetDefaultAddress());
}

void TJobRegistry::UnregisterJob(TJobPtr job)
{
    job->GetNode()->UnregisterJob(job);
    auto jobType = job->GetType();
    --RunningJobs_[jobType];

    auto jobState = job->GetState();
    switch (jobState) {
        case EJobState::Completed:
            ++JobsCompleted_[jobType];
            break;
        case EJobState::Failed:
            ++JobsFailed_[jobType];
            break;
        case EJobState::Aborted:
            ++JobsAborted_[jobType];
            break;
        default:
            break;
    }
    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto chunkId = job->GetChunkIdWithIndexes().Id;
    auto* chunk = chunkManager->FindChunk(chunkId);
    if (chunk) {
        chunk->RemoveJob(job);
        chunkManager->ScheduleChunkRefresh(chunk);
    }

    UpdateInterDCEdgeConsumption(job, job->GetNode()->GetDataCenter(), -1);

    YT_LOG_DEBUG("Job unregistered (JobId: %v, JobType: %v, Address: %v)",
        job->GetJobId(),
        job->GetType(),
        job->GetNode()->GetDefaultAddress());
}

bool TJobRegistry::IsOverdraft() const
{
    return JobThrottler_->IsOverdraft();
}

const TDynamicChunkManagerConfigPtr& TJobRegistry::GetDynamicConfig()
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    return configManager->GetConfig()->ChunkManager;
}

void TJobRegistry::OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
{
    JobThrottler_->Reconfigure(GetDynamicConfig()->JobThrottler);

    IgnoreEdgeCapacities_ = GetDynamicConfig()->InterDCLimits->IgnoreEdgeCapacities;

    UpdateInterDCEdgeCapacities();
}

void TJobRegistry::OverrideResourceLimits(TNodeResources* resourceLimits, const TNode& node)
{
    const auto& resourceLimitsOverrides = node.ResourceLimitsOverrides();
    #define XX(name, Name) \
        if (resourceLimitsOverrides.has_##name()) { \
            resourceLimits->set_##name(std::min(resourceLimitsOverrides.name(), resourceLimits->name())); \
        }
    ITERATE_NODE_RESOURCE_LIMITS_OVERRIDES(XX)
    #undef XX
}

void TJobRegistry::OnProfiling(TSensorBuffer* buffer) const
{
    for (auto jobType : TEnumTraits<EJobType>::GetDomainValues()) {
        if (jobType >= NJobTrackerClient::FirstMasterJobType && jobType <= NJobTrackerClient::LastMasterJobType) {
            buffer->PushTag({"job_type", FormatEnum(jobType)});

            buffer->AddGauge("/running_job_count", RunningJobs_[jobType]);
            buffer->AddCounter("/jobs_started", JobsStarted_[jobType]);
            buffer->AddCounter("/jobs_completed", JobsCompleted_[jobType]);
            buffer->AddCounter("/jobs_failed", JobsFailed_[jobType]);
            buffer->AddCounter("/jobs_aborted", JobsAborted_[jobType]);

            buffer->PopTag();
        }
    }

    for (const auto& srcPair : InterDCEdgeConsumption_) {
        const auto* src = srcPair.first;
        buffer->PushTag({"source_data_center", src ? src->GetName() : "null"});

        for (const auto& dstPair : srcPair.second) {
            const auto* dst = dstPair.first;
            buffer->PushTag({"destination_data_center", dst ? dst->GetName() : "null"});

            const auto consumption = dstPair.second;
            buffer->AddGauge("/inter_dc_edge_consumption", consumption);

            buffer->PopTag();
        }

        buffer->PopTag();
    }

    for (const auto& srcPair : InterDCEdgeCapacities_) {
        const auto* src = srcPair.first;
        buffer->PushTag({"source_data_center", src ? src->GetName() : "null"});

        for (const auto& dstPair : srcPair.second) {
            const auto* dst = dstPair.first;
            buffer->PushTag({"destination_data_center", dst ? dst->GetName() : "null"});

            const auto capacity = dstPair.second;
            buffer->AddGauge("/inter_dc_edge_capacity", capacity);

            buffer->PopTag();
        }

        buffer->PopTag();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
