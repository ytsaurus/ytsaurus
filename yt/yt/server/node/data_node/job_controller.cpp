#include "job_controller.h"

#include "bootstrap.h"
#include "job.h"
#include "master_connector.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/exec_node/bootstrap.h>
#include <yt/yt/server/node/exec_node/job_controller.h>

#include <yt/yt/server/lib/chunk_server/helpers.h>
#include <yt/yt/server/lib/exec_node/config.h>
#include <yt/yt/server/lib/job_agent/config.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/client/job_tracker_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NDataNode {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NYTree;
using namespace NYson;
using namespace NClusterNode;
using namespace NChunkServer;
using namespace NJobAgent;
using namespace NJobTrackerClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;

using NChunkServer::NProto::TJobSpec;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

class TJobController
    : public IJobController
{
public:
    DEFINE_SIGNAL_OVERRIDE(void(const TMasterJobBasePtr& job), JobFinished);

public:
    TJobController(
        IBootstrapBase* bootstrap)
        : Bootstrap_(bootstrap)
        , DynamicConfig_(New<TJobControllerDynamicConfig>())
        , Profiler_("/job_controller")
    {
        YT_VERIFY(Bootstrap_);

        VERIFY_INVOKER_THREAD_AFFINITY(Bootstrap_->GetJobInvoker(), JobThread);

        Profiler_.AddProducer("", ActiveJobCountBuffer_);
    }

    void Initialize() override
    {
        JobResourceManager_ = Bootstrap_->GetJobResourceManager();
        JobResourceManager_->RegisterResourcesConsumer(
            BIND_NO_PROPAGATE(&TJobController::OnResourceReleased, MakeWeak(this))
                .Via(Bootstrap_->GetJobInvoker()),
            EResourcesConsumerType::MasterJob);

        ProfilingExecutor_ = New<TPeriodicExecutor>(
            Bootstrap_->GetJobInvoker(),
            BIND_NO_PROPAGATE(&TJobController::OnProfiling, MakeWeak(this)),
            DynamicConfig_.Acquire()->ProfilingPeriod);
        ProfilingExecutor_->Start();

        auto jobsProfiler = DataNodeProfiler.WithPrefix("/master_jobs");

        MasterJobSensors_.AdaptivelyRepairedChunksCounter = jobsProfiler.Counter("/adaptively_repaired_chunks");
        MasterJobSensors_.TotalRepairedChunksCounter = jobsProfiler.Counter("/total_repaired_chunks");
        MasterJobSensors_.FailedRepairChunksCounter = jobsProfiler.Counter("/failed_repair_chunks");
    }

    std::vector<TMasterJobBasePtr> GetJobs() const
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        std::vector<TMasterJobBasePtr> result;
        result.reserve(GetActiveJobCount());

        for (const auto& [jobTrackerAddress, jobMap] : JobMaps_) {
            for (const auto& [id, job] : jobMap) {
                result.push_back(job);
            }
        }

        return result;
    }

    std::vector<TMasterJobBasePtr> GetJobs(const TString& jobTrackerAddress) const
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto jobMapIterator = JobMaps_.find(jobTrackerAddress);
        if (jobMapIterator == JobMaps_.end()) {
            return {};
        }

        const auto& jobMap = jobMapIterator->second;

        std::vector<TMasterJobBasePtr> result;
        result.reserve(jobMap.size());

        for (const auto& [id, job] : jobMap) {
            result.push_back(job);
        }

        return result;
    }

    TFuture<void> PrepareHeartbeatRequest(
        TCellTag cellTag,
        const TString& jobTrackerAddress,
        const TReqHeartbeatPtr& request) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return
            BIND(&TJobController::DoPrepareHeartbeatRequest, MakeStrong(this))
                .AsyncVia(Bootstrap_->GetJobInvoker())
                .Run(cellTag, jobTrackerAddress, request);
    }

    TFuture<void> ProcessHeartbeatResponse(
        const TString& jobTrackerAddress,
        const TRspHeartbeatPtr& response) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return BIND(&TJobController::DoProcessHeartbeatResponse, MakeStrong(this))
            .AsyncVia(Bootstrap_->GetJobInvoker())
            .Run(jobTrackerAddress, response);
    }

    TMasterJobBasePtr FindJob(const TString& jobTrackerAddress, NChunkServer::TJobId jobId) const
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto jobMapIt = JobMaps_.find(jobTrackerAddress);
        if (jobMapIt == JobMaps_.end()) {
            return nullptr;
        }

        auto& jobMap = jobMapIt->second;

        auto it = jobMap.find(jobId);
        return it == jobMap.end() ? nullptr : it->second;
    }

    void ScheduleStartJobs() override
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        if (std::exchange(StartJobsScheduled_, true)) {
            return;
        }

        Bootstrap_->GetJobInvoker()->Invoke(BIND(
            &TJobController::StartWaitingJobs,
            MakeWeak(this)));
    }

    int GetActiveJobCount() const
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        int totalJobCount = 0;
        for (const auto& [jobTrackerAddress, jobMap] : JobMaps_) {
            totalJobCount += std::ssize(jobMap);
        }

        return totalJobCount;
    }

    IYPathServicePtr GetOrchidService() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return IYPathService::FromProducer(BIND_NO_PROPAGATE(
            &TJobController::BuildOrchid,
            MakeStrong(this)));
    }


    void OnDynamicConfigChanged(
        const TJobControllerDynamicConfigPtr& newConfig) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_VERIFY(newConfig);

        Bootstrap_->GetControlInvoker()->Invoke(
            BIND([
                this,
                this_ = MakeStrong(this),
                newConfig = std::move(newConfig)
            ] {
                ProfilingExecutor_->SetPeriod(newConfig->ProfilingPeriod);

                DynamicConfig_.Store(std::move(newConfig));
            }));
    }

private:
    NClusterNode::IBootstrapBase* const Bootstrap_;
    TAtomicIntrusivePtr<TJobControllerDynamicConfig> DynamicConfig_;

    TJobResourceManagerPtr JobResourceManager_;

    TMasterJobSensors MasterJobSensors_;

    THashMap<TString, THashMap<NChunkServer::TJobId, TMasterJobBasePtr>> JobMaps_;

    bool StartJobsScheduled_ = false;

    TPeriodicExecutorPtr ProfilingExecutor_;

    TProfiler Profiler_;
    TBufferedProducerPtr ActiveJobCountBuffer_ = New<TBufferedProducer>();
    THashMap<EJobState, TCounter> JobFinalStateCounters_;

    DECLARE_THREAD_AFFINITY_SLOT(JobThread);

    void OnResourceReleased()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ScheduleStartJobs();
    }

    void StartWaitingJobs()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto resourceAcquiringContext = JobResourceManager_->GetResourceAcquiringContext();

        for (const auto& job : GetJobs()) {
            if (job->GetState() != EJobState::Waiting) {
                continue;
            }

            auto jobId = job->GetId();
            YT_LOG_DEBUG("Trying to start job (JobId: %v)", jobId);

            if (!resourceAcquiringContext.TryAcquireResourcesFor(StaticPointerCast<TResourceHolder>(job))) {
                YT_LOG_DEBUG("Job was not started (JobId: %v)", jobId);
            } else {
                YT_LOG_DEBUG("Job started (JobId: %v)", jobId);
            }
        }

        StartJobsScheduled_ = false;
    }

    TMasterJobBasePtr CreateJob(
        NChunkServer::TJobId jobId,
        const TString& jobTrackerAddress,
        const TJobResources& resourceLimits,
        TJobSpec&& jobSpec)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto jobType = CheckedEnumCast<EJobType>(jobSpec.type());
        YT_LOG_FATAL_IF(
            jobType < FirstMasterJobType || jobType > LastMasterJobType,
            "Trying to create job with unexpected type (JobId: %v, JobType: %v)",
            jobId,
            jobType);

        auto job = NDataNode::CreateJob(
            jobId,
            std::move(jobSpec),
            jobTrackerAddress,
            resourceLimits,
            Bootstrap_->GetDataNodeBootstrap(),
            MasterJobSensors_);

        YT_LOG_INFO(
            "Master job created (JobId: %v, JobType: %v, JobTrackerAddress: %v)",
            jobId,
            jobType,
            jobTrackerAddress);

        auto waitingJobTimeout = DynamicConfig_.Acquire()->WaitingJobsTimeout;

        RegisterJob(jobId, jobTrackerAddress, job, waitingJobTimeout);

        return job;
    }

    void RegisterJob(
        const NChunkServer::TJobId jobId,
        const TString& jobTrackerAddress,
        const TMasterJobBasePtr& job,
        const TDuration waitingJobTimeout)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        EmplaceOrCrash(JobMaps_[jobTrackerAddress], jobId, job);

        job->SubscribeJobFinished(
            BIND_NO_PROPAGATE(&TJobController::OnJobFinished, MakeWeak(this), MakeWeak(job))
                .Via(Bootstrap_->GetJobInvoker()));

        ScheduleStartJobs();

        TDelayedExecutor::Submit(
            BIND(&TJobController::OnWaitingJobTimeout, MakeWeak(this), MakeWeak(job), waitingJobTimeout),
            waitingJobTimeout,
            Bootstrap_->GetJobInvoker());
    }

    void OnWaitingJobTimeout(const TWeakPtr<TMasterJobBase>& weakJob, TDuration waitingJobTimeout)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto job = weakJob.Lock();
        if (!job) {
            return;
        }

        if (job->GetState() == EJobState::Waiting) {
            job->Abort(TError(NExecNode::EErrorCode::WaitingJobTimeout, "Job waiting has timed out")
                << TErrorAttribute("timeout", waitingJobTimeout));
        }
    }

    void OnJobFinished(const TWeakPtr<TMasterJobBase>& weakJob)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto job = weakJob.Lock();
        if (!job) {
            return;
        }

        if (job->IsUrgent()) {
            YT_LOG_DEBUG("Urgent job has finished, scheduling out-of-order job heartbeat (JobId: %v, JobType: %v)",
                job->GetId(),
                job->GetType());
            ScheduleHeartbeat(job);
        }

        if (!job->IsStarted()) {
            return;
        }

        auto* jobFinalStateCounter = GetJobFinalStateCounter(job->GetState());
        jobFinalStateCounter->Increment();

        JobFinished_.Fire(job);
    }

    TCounter* GetJobFinalStateCounter(EJobState state)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        auto it = JobFinalStateCounters_.find(state);
        if (it == JobFinalStateCounters_.end()) {
            auto counter = Profiler_
                .WithTag("state", FormatEnum(state))
                .WithTag("origin", FormatEnum(EJobOrigin::Master))
                .Counter("/job_final_state");

            it = JobFinalStateCounters_.emplace(state, counter).first;
        }

        return &it->second;
    }

    void ScheduleHeartbeat(const TMasterJobBasePtr& job)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto* bootstrap = Bootstrap_->GetDataNodeBootstrap();
        const auto& masterConnector = bootstrap->GetMasterConnector();
        masterConnector->ScheduleJobHeartbeat(job->GetJobTrackerAddress());
    }

    void FillJobStatus(NChunkServer::NProto::TJobStatus* status, const TMasterJobBasePtr& job)
    {
        using NYT::ToProto;

        ToProto(status->mutable_job_id(), job->GetId());
        status->set_job_type(static_cast<int>(job->GetType()));
        status->set_state(static_cast<int>(job->GetState()));
    }

    void DoPrepareHeartbeatRequest(
        TCellTag cellTag,
        const TString& jobTrackerAddress,
        const TReqHeartbeatPtr& request)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        request->set_node_id(ToProto<ui32>(Bootstrap_->GetNodeId()));
        ToProto(request->mutable_node_descriptor(), Bootstrap_->GetLocalDescriptor());
        *request->mutable_resource_limits() = ToNodeResources(JobResourceManager_->GetResourceLimits());
        *request->mutable_resource_usage() = ToNodeResources(JobResourceManager_->GetResourceUsage(/*includeWaiting*/ true));

        *request->mutable_disk_resources() = JobResourceManager_->GetDiskResources();

        for (const auto& job : GetJobs(jobTrackerAddress)) {
            auto jobId = job->GetIdAsGuid();

            YT_VERIFY(job->GetJobTrackerAddress() == jobTrackerAddress);

            YT_VERIFY(CellTagFromId(jobId) == cellTag);

            auto* jobStatus = request->add_jobs();

            FillJobStatus(jobStatus, job);
            switch (job->GetState()) {
                case EJobState::Running:
                    *jobStatus->mutable_resource_usage() = ToNodeResources(job->GetResourceUsage());
                    break;

                case EJobState::Completed:
                case EJobState::Aborted:
                case EJobState::Failed:
                    *jobStatus->mutable_result() = job->GetResult();
                    break;

                default:
                    break;
            }
        }
    }

    void DoProcessHeartbeatResponse(
        const TString& jobTrackerAddress,
        const TRspHeartbeatPtr& response)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        for (const auto& protoJobToRemove : response->jobs_to_remove()) {
            auto jobToRemove = FromProto<TJobToRemove>(protoJobToRemove);
            auto jobId = jobToRemove.JobId;

            if (auto job = FindJob(jobTrackerAddress, jobId)) {
                RemoveJob(std::move(job));
            } else {
                YT_LOG_WARNING("Requested to remove a non-existent job (JobId: %v)",
                    jobId);
            }
        }

        for (const auto& protoJobToAbort : response->jobs_to_abort()) {
            auto jobToAbort = FromProto<TJobToAbort>(protoJobToAbort);

            if (auto job = FindJob(jobTrackerAddress, jobToAbort.JobId)) {
                AbortJob(job);
            } else {
                YT_LOG_WARNING("Requested to abort a non-existent job (JobId: %v)",
                    jobToAbort.JobId);
            }
        }

        YT_VERIFY(std::ssize(response->Attachments()) == response->jobs_to_start_size());
        auto dynamicConfig = DynamicConfig_.Acquire();
        int attachmentIndex = 0;
        for (const auto& startInfo : response->jobs_to_start()) {
            auto jobId = FromProto<NChunkServer::TJobId>(startInfo.job_id());
            YT_LOG_DEBUG("Job spec received (JobId: %v, JobTrackerAddress: %v)",
                jobId,
                jobTrackerAddress);

            const auto& attachment = response->Attachments()[attachmentIndex];

            TJobSpec spec;
            DeserializeProtoWithEnvelope(&spec, attachment);

            const auto& resourceLimits = startInfo.resource_limits();

            auto jobResourceLimits = FromNodeResources(resourceLimits);

            if (!dynamicConfig->AccountMasterMemoryRequest) {
                // COMPAT(don-dron): Remove memory request on master. Only for repair jobs (256Mb).
                // See set_system_memory in server/master/chunk_server/chunk_replicator.cpp::TRepairJob.
                jobResourceLimits.SystemMemory = 0;
            }

            CreateJob(
                jobId,
                jobTrackerAddress,
                jobResourceLimits,
                std::move(spec));

            ++attachmentIndex;
        }
    }

    void AbortJob(const TMasterJobBasePtr& job)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_INFO("Aborting job (JobId: %v)",
            job->GetId());

        TError error("Job aborted by master request");

        job->Abort(error);
    }

    void RemoveJob(TMasterJobBasePtr job)
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        YT_LOG_FATAL_UNLESS(
            IsJobFinished(job->GetState()),
            "Removing job in unexpected state (State: %v, JobId: %v)",
            job->GetState(),
            job->GetId());

        auto& jobMap = JobMaps_[job->GetJobTrackerAddress()];

        auto jobId = job->GetId();

        EraseOrCrash(jobMap, jobId);

        if (jobMap.empty()) {
            EraseOrCrash(JobMaps_, job->GetJobTrackerAddress());
        }

        YT_LOG_INFO("Job removed (JobId: %v)", job->GetId());
    }

    void OnProfiling()
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        ActiveJobCountBuffer_->Update([this] (ISensorWriter* writer) {
            TWithTagGuard tagGuard(writer, "origin", FormatEnum(EJobOrigin::Master));
            writer->AddGauge("/active_job_count", GetJobs().size());
        });
    }

    static void BuildJobsInfo(const std::vector<TBriefJobInfo>& jobsInfo, TFluentAny fluent)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        fluent.DoMapFor(
            jobsInfo,
            [&] (TFluentMap fluent, const TBriefJobInfo& jobInfo) {
                jobInfo.BuildOrchid(fluent);
            });
    }

    auto DoGetStateSnapshot() const
    {
        VERIFY_THREAD_AFFINITY(JobThread);

        std::vector<TBriefJobInfo> jobsInfo;
        jobsInfo.reserve(GetActiveJobCount());

        for (const auto& [jobTrackerAddress, jobMap] : JobMaps_){
            for (auto [id, job] : jobMap) {
                jobsInfo.push_back(job->GetBriefInfo());
            }
        }

        return jobsInfo;
    }

    auto GetStateSnapshot() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto snapshotOrError = WaitFor(BIND(
            &TJobController::DoGetStateSnapshot,
            MakeStrong(this))
            .AsyncVia(Bootstrap_->GetJobInvoker())
            .Run());

        YT_LOG_FATAL_UNLESS(
            snapshotOrError.IsOK(),
            snapshotOrError,
            "Unexpected faliure while making data node job controller info snapshot");

        return std::move(snapshotOrError.Value());
    }

    void BuildOrchid(IYsonConsumer* consumer) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto jobsInfo = GetStateSnapshot();

        BuildYsonFluently(consumer).BeginMap()
            .Item("active_job_count").Value(std::ssize(jobsInfo))
            .Item("active_jobs").Do(std::bind(
                &TJobController::BuildJobsInfo,
                jobsInfo,
                std::placeholders::_1))
        .EndMap();
    }
};

////////////////////////////////////////////////////////////////////////////////

IJobControllerPtr CreateJobController(NClusterNode::IBootstrapBase* bootstrap)
{
    return New<TJobController>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
