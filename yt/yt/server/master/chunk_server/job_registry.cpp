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
{ }

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
    for (auto& typeQueues : FinishedJobQueues_) {
        for (auto& stateQueues : typeQueues) {
            stateQueues.clear();
        }
    }
    LastFinishedJobs_ = {};
}

void TJobRegistry::RegisterFinishedJob(const TJobPtr& job)
{
    auto chunkId = job->GetChunkIdWithIndexes().Id;
    LastFinishedJobs_[chunkId] = job;
    auto& queue = FinishedJobQueues_[job->GetType()][job->GetState()];
    queue.push(job);

    while (std::ssize(queue) > FinishedJobQueueSizeLimit_) {
        const auto& job = queue.front();
        auto chunkId = job->GetChunkIdWithIndexes().Id;
        auto it = LastFinishedJobs_.find(chunkId);
        if (it != LastFinishedJobs_.end()) {
            LastFinishedJobs_.erase(it);
        }
        queue.pop();
    }
}

TJobPtr TJobRegistry::FindLastFinishedJob(TChunkId chunkId) const
{
    auto it = LastFinishedJobs_.find(chunkId);
    return it != LastFinishedJobs_.end() ? it->second : nullptr;
}

void TJobRegistry::RegisterJob(const TJobPtr& job)
{
    const auto& nodeTracker = Bootstrap_->GetNodeTracker();
    auto* node = nodeTracker->FindNodeByAddress(job->NodeAddress());
    node->RegisterJob(job);

    auto jobType = job->GetType();
    ++RunningJobs_[jobType];
    ++JobsStarted_[jobType];

    const auto& chunkManager = Bootstrap_->GetChunkManager();
    auto chunkId = job->GetChunkIdWithIndexes().Id;
    auto* chunk = chunkManager->FindChunk(chunkId);
    if (chunk) {
        chunk->AddJob(job);
    }

    JobThrottler_->Acquire(1);

    YT_LOG_TRACE("Job registered (JobId: %v, JobType: %v, Address: %v)",
        job->GetJobId(),
        job->GetType(),
        job->NodeAddress());
}

void TJobRegistry::OnJobFinished(TJobPtr job)
{
    const auto& nodeTracker = Bootstrap_->GetNodeTracker();
    auto* node = nodeTracker->FindNodeByAddress(job->NodeAddress());
    node->UnregisterJob(job);
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
        RegisterFinishedJob(job);
        chunkManager->ScheduleChunkRefresh(chunk);
    }

    YT_LOG_TRACE("Job unregistered (JobId: %v, JobType: %v, Address: %v)",
        job->GetJobId(),
        job->GetType(),
        job->NodeAddress());
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
    FinishedJobQueueSizeLimit_ = GetDynamicConfig()->FinishedJobsQueueSize;
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

int TJobRegistry::GetJobCount(EJobType type) const
{
    return RunningJobs_[type];
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
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
