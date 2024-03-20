#include "job_registry.h"

#include "chunk_manager.h"
#include "config.h"
#include "job.h"
#include "helpers.h"

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
using namespace NJobTrackerClient;
using namespace NProfiling;
using namespace NCypressClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TJobRegistry
    : public IJobRegistry
{
public:
    explicit TJobRegistry(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , JobThrottler_(CreateJobThrottler())
        , PerTypeJobThrottlers_(CreatePerTypeJobThrottlers())
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND(&TJobRegistry::OnDynamicConfigChanged, MakeWeak(this)));
    }

    void RegisterJob(TJobPtr job) override
    {
        YT_VERIFY(ActiveEpochs_.contains(job->GetJobEpoch()));
        YT_VERIFY(job->NodeAddress());

        EmplaceOrCrash(IdToJob_, job->GetJobId(), job);
        InsertOrCrash(NodeAddressToJobs_[job->NodeAddress()], job);
        InsertOrCrash(EpochToJobs_[job->GetJobEpoch()], job);

        auto jobType = job->GetType();
        ++RunningJobs_[jobType];
        ++JobsStarted_[jobType];

        const auto& chunkManager = Bootstrap_->GetChunkManager();
        auto chunkId = job->GetChunkIdWithIndexes().Id;
        auto* chunk = chunkManager->FindChunk(chunkId);
        if (chunk) {
            chunk->AddJob(job);
        }

        const auto& perTypeJobThrottler = GetOrCrash(PerTypeJobThrottlers_, job->GetType());
        perTypeJobThrottler->Acquire(1);

        JobThrottler_->Acquire(1);

        YT_LOG_TRACE("Job registered (JobId: %v, JobType: %v, Address: %v)",
            job->GetJobId(),
            job->GetType(),
            job->NodeAddress());
    }

    void OnJobFinished(TJobPtr job) override
    {
        EraseOrCrash(IdToJob_, job->GetJobId());
        {
            auto& nodeJobs = NodeAddressToJobs_[job->NodeAddress()];
            EraseOrCrash(nodeJobs, job);
            if (nodeJobs.empty()) {
                EraseOrCrash(NodeAddressToJobs_, job->NodeAddress());
            }
        }
        {
            auto& epochJobs = EpochToJobs_[job->GetJobEpoch()];
            EraseOrCrash(epochJobs, job);
            if (epochJobs.empty()) {
                EraseOrCrash(EpochToJobs_, job->GetJobEpoch());
            }
        }

        auto jobType = job->GetType();
        --RunningJobs_[jobType];

        switch (job->GetState()) {
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

    TJobPtr FindJob(TJobId jobId) override
    {
        auto jobIt = IdToJob_.find(jobId);
        if (jobIt == IdToJob_.end()) {
            return nullptr;
        }

        const auto& job = jobIt->second;
        if (!ActiveEpochs_.find(job->GetJobEpoch())) {
            const auto& chunkManager = Bootstrap_->GetChunkManager();
            chunkManager->AbortAndRemoveJob(job);

            return nullptr;
        }

        return job;
    }

    TJobPtr FindLastFinishedJob(TChunkId chunkId) const override
    {
        auto it = LastFinishedJobs_.find(chunkId);
        return it != LastFinishedJobs_.end() ? it->second : nullptr;
    }

    const THashSet<TJobPtr>& GetNodeJobs(const TString& nodeAddress) const override
    {
        auto nodeIt = NodeAddressToJobs_.find(nodeAddress);
        if (nodeIt == NodeAddressToJobs_.end()) {
            const static THashSet<TJobPtr> EmptySet;
            return EmptySet;
        } else {
            return nodeIt->second;
        }
    }

    int GetJobCount(EJobType jobType) const override
    {
        return RunningJobs_[jobType];
    }

    TJobEpoch StartEpoch() override
    {
        auto epoch = NextEpoch_++;
        InsertOrCrash(ActiveEpochs_, epoch);

        YT_LOG_INFO("Job epoch started (Epoch: %v)",
            epoch);

        return epoch;
    }

    void OnEpochFinished(TJobEpoch epoch) override
    {
        EraseOrCrash(ActiveEpochs_, epoch);

        if (GetDynamicConfig()->AbortJobsOnEpochFinish) {
            const auto& chunkManager = Bootstrap_->GetChunkManager();

            auto jobs = EpochToJobs_[epoch];
            for (const auto& job : jobs) {
                chunkManager->AbortAndRemoveJob(job);
            }
            EpochToJobs_.erase(epoch);
        }

        YT_LOG_INFO("Job epoch finished (Epoch: %v)",
            epoch);
    }

    bool IsOverdraft() const override
    {
        return JobThrottler_->IsOverdraft();
    }

    bool IsOverdraft(EJobType jobType) const override
    {
        const auto& jobThrottler = GetOrCrash(PerTypeJobThrottlers_, jobType);

        return jobThrottler->IsOverdraft();
    }

    void OverrideResourceLimits(TNodeResources* resourceLimits, const TNode& node) override
    {
        const auto& resourceLimitsOverrides = node.ResourceLimitsOverrides();
        #define XX(name, Name) \
            if (resourceLimitsOverrides.has_##name()) { \
                resourceLimits->set_##name(std::min(resourceLimitsOverrides.name(), resourceLimits->name())); \
            }
        ITERATE_NODE_RESOURCE_LIMITS_OVERRIDES(XX)
        #undef XX
    }

    void OnProfiling(TSensorBuffer* buffer) override
    {
        for (auto jobType : TEnumTraits<EJobType>::GetDomainValues()) {
            if (IsMasterJobType(jobType)) {
                TWithTagGuard tagGuard(buffer, "job_type", FormatEnum(jobType));
                buffer->AddGauge("/running_job_count", RunningJobs_[jobType]);
                buffer->AddCounter("/jobs_started", JobsStarted_[jobType]);
                buffer->AddCounter("/jobs_completed", JobsCompleted_[jobType]);
                buffer->AddCounter("/jobs_failed", JobsFailed_[jobType]);
                buffer->AddCounter("/jobs_aborted", JobsAborted_[jobType]);
            }
        }
    }

private:
    TBootstrap* const Bootstrap_;

    THashMap<TJobId, TJobPtr> IdToJob_;
    THashMap<TString, THashSet<TJobPtr>> NodeAddressToJobs_;
    THashMap<TJobEpoch, THashSet<TJobPtr>> EpochToJobs_;

    TEnumIndexedArray<EJobType, TEnumIndexedArray<EJobState, TRingQueue<TJobPtr>>> FinishedJobQueues_;
    THashMap<TChunkId, TJobPtr> LastFinishedJobs_;

    TJobEpoch NextEpoch_ = 1;
    THashSet<TJobEpoch> ActiveEpochs_;

    const IReconfigurableThroughputThrottlerPtr JobThrottler_;

    using TPerTypeJobThrottlers = THashMap<EJobType, IReconfigurableThroughputThrottlerPtr>;
    const TPerTypeJobThrottlers PerTypeJobThrottlers_;

    using TJobCounters = TEnumIndexedArray<EJobType, i64, FirstMasterJobType, LastMasterJobType>;
    TJobCounters RunningJobs_;
    TJobCounters JobsStarted_;
    TJobCounters JobsCompleted_;
    TJobCounters JobsFailed_;
    TJobCounters JobsAborted_;

    void RegisterFinishedJob(const TJobPtr& finishedJob)
    {
        auto job = MummifyJob(finishedJob);
        auto chunkId = job->GetChunkIdWithIndexes().Id;
        LastFinishedJobs_[chunkId] = job;
        auto& queue = FinishedJobQueues_[job->GetType()][job->GetState()];
        queue.push(job);

        auto finishedJobQueueSizeLimit = GetDynamicConfig()->FinishedJobsQueueSize;
        while (std::ssize(queue) > finishedJobQueueSizeLimit) {
            const auto& job = queue.front();
            auto chunkId = job->GetChunkIdWithIndexes().Id;
            auto it = LastFinishedJobs_.find(chunkId);
            if (it != LastFinishedJobs_.end()) {
                LastFinishedJobs_.erase(it);
            }
            queue.pop();
        }
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        for (auto jobType : TEnumTraits<EJobType>::GetDomainValues()) {
            if (IsMasterJobType(jobType)) {
                auto& jobThrottler = GetOrCrash(PerTypeJobThrottlers_, jobType);
                auto jobThrottlerConfig = GetDynamicConfig()->JobTypeToThrottler.find(jobType)->second;
                jobThrottler->Reconfigure(std::move(jobThrottlerConfig));
            }
        }
        JobThrottler_->Reconfigure(GetDynamicConfig()->JobThrottler);
    }

    const TDynamicChunkManagerConfigPtr& GetDynamicConfig() const
    {
        const auto& configManager = Bootstrap_->GetConfigManager();
        return configManager->GetConfig()->ChunkManager;
    }

    static IReconfigurableThroughputThrottlerPtr CreateJobThrottler()
    {
        return CreateReconfigurableThroughputThrottler(
            New<TThroughputThrottlerConfig>(),
            ChunkServerLogger,
            ChunkServerProfiler.WithPrefix("/job_throttler"));
    }

    static TPerTypeJobThrottlers CreatePerTypeJobThrottlers()
    {
        TPerTypeJobThrottlers throttlers;
        for (auto jobType : TEnumTraits<EJobType>::GetDomainValues()) {
            if (IsMasterJobType(jobType)) {
                auto throttler = CreateReconfigurableThroughputThrottler(
                    New<TThroughputThrottlerConfig>(),
                    ChunkServerLogger,
                    ChunkServerProfiler.WithPrefix(Format("/per_type_job_throttler/%lv", jobType)));
                EmplaceOrCrash(throttlers, jobType, std::move(throttler));
            }
        }

        return throttlers;
    }
};

DEFINE_REFCOUNTED_TYPE(TJobRegistry)

////////////////////////////////////////////////////////////////////////////////

IJobRegistryPtr CreateJobRegistry(TBootstrap* bootstrap)
{
    return New<TJobRegistry>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
