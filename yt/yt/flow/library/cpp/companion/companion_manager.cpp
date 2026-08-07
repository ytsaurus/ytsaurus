#include "companion_manager.h"

#include "companion_client_detail.h"
#include "companion_entrypoint.h"
#include "companion_process_manager.h"
#include "companion_singleton_state.h"
#include "config.h"
#include "private.h"

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

void TCompanionManagerParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("timeout", &TThis::Timeout)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("backoff", &TThis::Backoff)
        .Default(TExponentialBackoffOptions{
            .InvocationCount = 30,
            .MinBackoff = TDuration::MilliSeconds(500),
            .MaxBackoff = TDuration::Seconds(10),
        });
    registrar.Parameter("entrypoint", &TThis::Entrypoint)
        .DefaultCtor([] {
            return New<TCompanionEntrypoint>();
        });
    registrar.Parameter("init_backoff", &TThis::InitBackoff)
        .Default(TExponentialBackoffOptions{
            .InvocationCount = std::numeric_limits<decltype(TThis::InitBackoff.InvocationCount)>::max(),
            .MinBackoff = TDuration::Seconds(5),
            .MaxBackoff = TDuration::Seconds(30),
        });
    registrar.Parameter("health_check_interval", &TThis::HealthCheckInterval)
        .Default(TDuration::Seconds(20));
    registrar.Parameter("startup_grace_period", &TThis::StartupGracePeriod)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("metrics_collection_interval", &TThis::MetricsCollectionInterval)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("job_reconciliation_period", &TThis::JobReconciliationPeriod)
        .Default(TDuration::Seconds(15))
        .GreaterThan(TDuration::Zero());
    registrar.Parameter("restart_delay", &TThis::RestartDelay)
        .Default(TDuration::MilliSeconds(100));
}

////////////////////////////////////////////////////////////////////////////////

TCompanionManager::TCompanionManager(TResourceContextPtr context, TDynamicResourceContextPtr dynamicContext)
    : TResourceBase(context, std::move(dynamicContext))
    , CompanionConfig_(GetCompanionExecutionConfig())
    , CompanionAddress_(Format("0.0.0.0:%v", CompanionConfig_->Port))
    , CompanionClient_(CreateCompanionClient(GetContext()->StatusProfiler->WithPrefix("/common_companion_client")))
    , Profiler_(context->Profiler.WithPrefix("/companion_manager"))
    , OrphanJobRemovalCounter_(Profiler_.Counter("/orphan_job_removals"))
    , JobRemovalFailureCounter_(Profiler_.Counter("/job_removal_failures"))
{ }

TCompanionManager::~TCompanionManager()
{
    if (JobReconciliationExecutor_) {
        YT_UNUSED_FUTURE(JobReconciliationExecutor_->Stop());
    }
    if (ProcessManager_) {
        ProcessManager_->Shutdown();
    }
}

ICompanionClientPtr TCompanionManager::CreateCompanionClient(IStatusProfilerPtr statusProfiler)
{
    return New<TCompanionClient>(
        CompanionAddress_,
        GetParameters()->Timeout,
        GetParameters()->Backoff,
        statusProfiler);
}

TProcessManagerBasePtr TCompanionManager::CreateProcessManager()
{
    return New<TCompanionProcessManager>(
        GetContext()->Invoker,
        CompanionClient_,
        GetParameters()->InitBackoff,
        GetParameters()->RestartDelay,
        GetParameters()->HealthCheckInterval,
        GetParameters()->StartupGracePeriod,
        GetParameters()->MetricsCollectionInterval,
        Logger,
        Profiler_,
        GetContext()->StatusProfiler->WithPrefix("/companion_process_manager"),
        GetParameters()->Entrypoint);
}

TFuture<void> TCompanionManager::Load(const THashMap<TResourceId, IResourcePtr>& /*dependencies*/)
{
    ProcessManager_ = CreateProcessManager();
    JobReconciliationExecutor_ = New<NConcurrency::TPeriodicExecutor>(
        GetContext()->Invoker,
        BIND(&TCompanionManager::ReconcileJobs, MakeWeak(this)),
        GetParameters()->JobReconciliationPeriod);
    return BIND([this, weakThis = MakeWeak(this)] () {
        if (auto strongThis = weakThis.Lock()) {
            ProcessManager_->Start();
            JobReconciliationExecutor_->Start();
        }
    })
        .AsyncVia(GetContext()->Invoker)
        .Run();
}

void TCompanionManager::RegisterLiveJob(const TJobId& jobId, ICompanionClientPtr client)
{
    auto guard = Guard(LiveJobsLock_);
    LiveJobs_[jobId] = TLiveJob{.Client = std::move(client)};
}

void TCompanionManager::UnregisterLiveJob(const TJobId& jobId, const ICompanionClientPtr& client)
{
    {
        auto guard = Guard(LiveJobsLock_);
        auto it = LiveJobs_.find(jobId);
        if (it == LiveJobs_.end() || it->second.Client != client) {
            // A newer computation incarnation of this job id registered
            // itself before this destructor ran; the entry and the companion
            // registration now belong to it.
            return;
        }
        // Keep the entry as a removal route rather than erasing it: this
        // client may be the only channel reaching its fan-out child, so the
        // reconcile pass must be able to query it until the companion
        // provably no longer holds the job.
        it->second.Removing = true;
    }
    // One prompt attempt so state is released right away; a lost removal is
    // repaired by the reconcile pass. The client is captured weakly: a strong
    // capture would cycle through a never-resolving future's subscriber list
    // back to the client and leak both.
    client->RemoveJob(jobId).Subscribe(BIND(
        [weakThis = MakeWeak(this), weakClient = MakeWeak(client.Get()), jobId, Logger = Logger] (
            const TError& error) {
            auto this_ = weakThis.Lock();
            if (!this_) {
                return;
            }
            if (error.IsOK()) {
                auto client = weakClient.Lock();
                if (!client) {
                    // The route no longer holds this client, so the entry
                    // cannot be ours to erase.
                    return;
                }
                auto guard = Guard(this_->LiveJobsLock_);
                auto it = this_->LiveJobs_.find(jobId);
                if (it != this_->LiveJobs_.end() &&
                    it->second.Removing &&
                    it->second.Client == client)
                {
                    this_->LiveJobs_.erase(it);
                }
                return;
            }
            this_->JobRemovalFailureCounter_.Increment();
            YT_TLOG_DEBUG("Failed to remove job from companion, the reconcile pass repairs it")
                .With("JobId", jobId)
                .With(error);
        }));
}

void TCompanionManager::ReconcileJobs()
{
    // One client per distinct companion process. The manager's own client
    // always participates: a worker drained to zero jobs has no live-job
    // clients exactly when the most abandoned registrations are in flight.
    // Clients with an unknown pid are queried until identified; each per-pid
    // group rotates its representative every pass, so a client whose channel
    // silently migrated to another process is eventually queried again and
    // its recorded pid corrected.
    std::vector<ICompanionClientPtr> clients;
    {
        auto guard = Guard(LiveJobsLock_);
        clients.push_back(CompanionClient_);
        THashMap<i64, std::vector<ICompanionClientPtr>> clientsByProcessId;
        for (const auto& [jobId, liveJob] : LiveJobs_) {
            if (liveJob.ProcessId == 0) {
                clients.push_back(liveJob.Client);
            } else {
                clientsByProcessId[liveJob.ProcessId].push_back(liveJob.Client);
            }
        }
        ++ReconcilePassIndex_;
        for (const auto& [processId, group] : clientsByProcessId) {
            clients.push_back(group[ReconcilePassIndex_ % std::ssize(group)]);
        }
    }
    for (auto& client : clients) {
        ReconcileClient(std::move(client));
    }
}

void TCompanionManager::ReconcileClient(ICompanionClientPtr client)
{
    client->ListJobs().Subscribe(BIND(
        [weakThis = MakeWeak(this), client, Logger = Logger] (
            const TErrorOr<TCompanionJobList>& reply) {
            auto this_ = weakThis.Lock();
            if (!this_) {
                return;
            }
            if (!reply.IsOK()) {
                YT_TLOG_DEBUG("Failed to list companion jobs, the next reconcile pass retries")
                    .With(reply);
                return;
            }
            this_->RemoveOrphanJobs(client, reply.Value());
        }));
}

void TCompanionManager::RemoveOrphanJobs(const ICompanionClientPtr& client, const TCompanionJobList& jobList)
{
    std::vector<TJobId> orphans;
    {
        auto guard = Guard(LiveJobsLock_);
        for (auto& [jobId, liveJob] : LiveJobs_) {
            if (liveJob.Client == client) {
                liveJob.ProcessId = jobList.ProcessId;
            }
        }
        THashSet<TJobId> listedJobIds;
        for (const auto& jobId : jobList.JobIds) {
            listedJobIds.insert(jobId);
            auto it = LiveJobs_.find(jobId);
            if (it == LiveJobs_.end() || it->second.Removing) {
                orphans.push_back(jobId);
                continue;
            }
            const auto& liveJob = it->second;
            // A live job listed by a process its own channel does not reach
            // is a stale copy left behind by a channel migration; served
            // later, it would answer with an outdated spec. An unidentified
            // channel (pid 0) is left alone until the rotation identifies it.
            if (liveJob.Client != client &&
                liveJob.ProcessId != 0 &&
                liveJob.ProcessId != jobList.ProcessId)
            {
                orphans.push_back(jobId);
            }
        }
        // A removal route whose own process no longer lists the job has done
        // its work.
        for (auto it = LiveJobs_.begin(); it != LiveJobs_.end();) {
            if (it->second.Removing &&
                it->second.Client == client &&
                !listedJobIds.contains(it->first))
            {
                LiveJobs_.erase(it++);
            } else {
                ++it;
            }
        }
    }
    if (orphans.empty()) {
        return;
    }
    YT_TLOG_INFO("Removing orphan companion jobs")
        .With("Count", std::ssize(orphans))
        .With("ProcessId", jobList.ProcessId);
    for (const auto& jobId : orphans) {
        OrphanJobRemovalCounter_.Increment();
        client->RemoveJob(jobId).Subscribe(BIND(
            [jobId, Logger = Logger] (const TError& error) {
                YT_TLOG_DEBUG_UNLESS(error.IsOK(), "Failed to remove orphan companion job, the next reconcile pass retries")
                    .With("JobId", jobId)
                    .With(error);
            }));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
