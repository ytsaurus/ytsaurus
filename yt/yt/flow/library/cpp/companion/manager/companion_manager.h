#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/resources/resource_base.h>

#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/misc/config.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

//! YSON-serializable parameters for the companion manager.
struct TCompanionManagerParameters
    : public NYTree::TYsonStruct
{
    //! Timeout for individual RPC calls to the companion process.
    TDuration Timeout;

    //! Exponential backoff options used when retrying failed companion requests.
    TExponentialBackoffOptions Backoff;

    //! Description of the companion executable and its arguments.
    TCompanionEntrypointPtr Entrypoint;

    //! Exponential backoff options used while waiting for the companion to become ready after startup.
    TExponentialBackoffOptions InitBackoff;

    //! Interval between periodic health-check pings to the companion process.
    TDuration HealthCheckInterval;

    //! Window after spawning a companion incarnation during which health check failures
    //! do not restart the process, as long as it is alive and has not passed its first health check.
    TDuration StartupGracePeriod;

    //! Interval between periodic metrics collection from the companion process.
    TDuration MetricsCollectionInterval;

    //! Interval between reconcile passes removing companion jobs whose
    //! computation no longer exists in this worker process.
    TDuration JobReconciliationPeriod;

    //! Delay before restarting the companion process after a failure.
    TDuration RestartDelay;

    REGISTER_YSON_STRUCT(TCompanionManagerParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCompanionManagerParameters);

////////////////////////////////////////////////////////////////////////////////

//! Resource that launches and supervises a companion process described by a
//! language-agnostic entrypoint and exposes an RPC client to it.
/*!
 *  Resolves the companion port from the singleton state, constructs a gRPC client
 *  bound to the local companion address, and launches and supervises the companion
 *  process with auto-restart and health-checks.
 */
class TCompanionManager
    : public TResourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TCompanionManagerParameters);

    TCompanionManager(TResourceContextPtr context, TDynamicResourceContextPtr dynamicContext);

    ~TCompanionManager() override;

    //! Creates a new companion RPC client with the given status profiler.
    ICompanionClientPtr CreateCompanionClient(IStatusProfilerPtr statusProfiler);

    //! Starts the companion process and waits for it to become ready.
    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& dependencies) override;

    //! Records that |jobId|'s computation exists in this worker process.
    /*!
     *  Must be called strictly before the job's first registration is sent to
     *  the companion: the reconcile pass treats a companion job absent from
     *  the live set as garbage. |client| must be the job's own client, since
     *  a channel reaches exactly one companion process.
     */
    void RegisterLiveJob(const TJobId& jobId, ICompanionClientPtr client);

    //! Forgets a live job and sends one prompt best-effort removal; a lost
    //! one is repaired by the reconcile pass.
    /*!
     *  Never blocks and never throws: it is called from a destructor.
     *  |client| identifies the caller: a stale destructor whose job id was
     *  re-registered by a newer computation incarnation is a no-op.
     */
    void UnregisterLiveJob(const TJobId& jobId, const ICompanionClientPtr& client);

protected:
    //! Creates the process manager responsible for spawning and supervising the companion.
    virtual TProcessManagerBasePtr CreateProcessManager();

    //! Asks each companion process what jobs it holds and removes those whose
    //! computation no longer exists in this worker process.
    void ReconcileJobs();

    //! Companion config with port, monitoring port, cluster url and pipeline path.
    const TCompanionExecutionConfigPtr CompanionConfig_;

    //! Full local address (host:port) used to connect to the companion process.
    const std::string CompanionAddress_;

    //! gRPC client used to send requests to the companion process.
    ICompanionClientPtr CompanionClient_;

    //! Profiler scoped to this companion manager instance.
    NProfiling::TProfiler Profiler_;

    //! Process manager; constructed lazily in Load() once CreateProcessManager() is available.
    TProcessManagerBasePtr ProcessManager_;

private:
    struct TLiveJob
    {
        ICompanionClientPtr Client;
        //! Pid the client's channel last answered a ListJobs from; 0 until
        //! the first reply. Lets the reconcile pass query one client per
        //! companion process instead of one per job.
        i64 ProcessId = 0;
        //! The job's computation is destroyed and the entry survives only as
        //! a removal route; it is erased once the removal is acknowledged or
        //! its process no longer lists the job.
        bool Removing = false;
    };

    //! Lists the jobs one companion process holds and removes the orphans.
    void ReconcileClient(ICompanionClientPtr client);
    //! Removes listed jobs that are absent from the live set. The live
    //! snapshot is taken here, strictly after the reply arrived: a job that
    //! registered itself before the reply was built is either still live or
    //! already destroyed, so the difference contains no live job.
    void RemoveOrphanJobs(const ICompanionClientPtr& client, const TCompanionJobList& jobList);

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, LiveJobsLock_);
    THashMap<TJobId, TLiveJob> LiveJobs_;
    //! Rotates the representative queried within each per-pid client group.
    i64 ReconcilePassIndex_ = 0;

    NConcurrency::TPeriodicExecutorPtr JobReconciliationExecutor_;
    NProfiling::TCounter OrphanJobRemovalCounter_;
    NProfiling::TCounter JobRemovalFailureCounter_;
};

DEFINE_REFCOUNTED_TYPE(TCompanionManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
