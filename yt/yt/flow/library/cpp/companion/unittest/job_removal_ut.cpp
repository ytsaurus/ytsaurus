#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/companion_client.h>
#include <yt/yt/flow/library/cpp/companion/companion_manager.h>
#include <yt/yt/flow/library/cpp/companion/companion_model.h>
#include <yt/yt/flow/library/cpp/companion/companion_resource.h>
#include <yt/yt/flow/library/cpp/companion/process_manager_base.h>

#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NFlow::NCompanion {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

//! Companion client whose RemoveJob and ListJobs outcomes are scripted by the
//! test, standing in for one companion process.
class TFakeCompanionClient
    : public ICompanionClient
{
public:
    explicit TFakeCompanionClient(i64 processId)
        : ProcessId_(processId)
    { }

    //! Models the channel migrating to another fan-out child.
    void SetProcessId(i64 processId)
    {
        auto guard = Guard(Lock_);
        ProcessId_ = processId;
    }

    TFuture<void> RemoveJob(const TJobId& jobId) override
    {
        auto guard = Guard(Lock_);
        RemovedJobIds_.push_back(jobId);
        if (HangRemovals_) {
            // The promise must outlive this call: an abandoned one resolves
            // its future with an error instead of never answering.
            auto promise = NewPromise<void>();
            HangingPromises_.push_back(promise);
            return promise.ToFuture();
        }
        if (RemovalFailureCount_ > 0) {
            --RemovalFailureCount_;
            return MakeFuture(TError("Companion is not answering"));
        }
        HeldJobIds_.erase(jobId);
        return MakeFuture(TError());
    }

    TFuture<TCompanionJobList> ListJobs() override
    {
        auto guard = Guard(Lock_);
        ++ListCount_;
        auto reply = TCompanionJobList{
            .JobIds = {HeldJobIds_.begin(), HeldJobIds_.end()},
            .ProcessId = ProcessId_,
        };
        if (PendingListPromise_) {
            // The test releases the reply itself via ReleaseListReply().
            PendingReply_ = std::move(reply);
            return PendingListPromise_.ToFuture();
        }
        return MakeFuture(std::move(reply));
    }

    TCompanionResponsePtr DoProcessWithCompanionSync(
        const TCompanionProcessRequestPtr& /*companionRequest*/,
        const IExternalPerformanceMetricsReporterPtr& /*reporter*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TCompanionInfoPtr GetCompanionInfo() override
    {
        YT_UNIMPLEMENTED();
    }

    TCompanionPutJobResponsePtr PutJob(
        const TCompanionPutJobRequestPtr& /*putJobRequest*/,
        const IExternalPerformanceMetricsReporterPtr& /*reporter*/) override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<TCompanionResourceExecuteResponsePtr> ResourceExecute(
        const TResourceId& /*resourceId*/,
        ECompanionResourceCommand /*command*/,
        const NYson::TYsonString& /*argument*/) override
    {
        YT_UNIMPLEMENTED();
    }

    //! Seeds the fake registry, as if a registration reached this process.
    void HoldJob(const TJobId& jobId)
    {
        auto guard = Guard(Lock_);
        HeldJobIds_.insert(jobId);
    }

    //! Makes the next ListJobs return an unresolved future; the test resolves
    //! it later with ReleaseListReply(), modelling the reply in flight.
    void DelayNextListReply()
    {
        auto guard = Guard(Lock_);
        PendingListPromise_ = NewPromise<TCompanionJobList>();
    }

    void ReleaseListReply()
    {
        TPromise<TCompanionJobList> promise;
        TCompanionJobList reply;
        {
            auto guard = Guard(Lock_);
            promise = std::exchange(PendingListPromise_, TPromise<TCompanionJobList>());
            reply = std::move(PendingReply_);
        }
        promise.Set(std::move(reply));
    }

    //! Makes the next |count| removal attempts fail.
    void FailNextRemovals(int count)
    {
        auto guard = Guard(Lock_);
        RemovalFailureCount_ = count;
    }

    //! Makes every subsequent removal attempt hang, modelling a stuck companion.
    void HangRemovals()
    {
        auto guard = Guard(Lock_);
        HangRemovals_ = true;
    }

    std::vector<TJobId> GetRemovedJobIds() const
    {
        auto guard = Guard(Lock_);
        return RemovedJobIds_;
    }

    int GetRemovalCount() const
    {
        auto guard = Guard(Lock_);
        return std::ssize(RemovedJobIds_);
    }

    int GetListCount() const
    {
        auto guard = Guard(Lock_);
        return ListCount_;
    }

    bool HoldsJob(const TJobId& jobId) const
    {
        auto guard = Guard(Lock_);
        return HeldJobIds_.contains(jobId);
    }

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    i64 ProcessId_;
    int RemovalFailureCount_ = 0;
    bool HangRemovals_ = false;
    int ListCount_ = 0;
    THashSet<TJobId> HeldJobIds_;
    std::vector<TJobId> RemovedJobIds_;
    std::vector<TPromise<void>> HangingPromises_;
    TPromise<TCompanionJobList> PendingListPromise_;
    TCompanionJobList PendingReply_;
};

DECLARE_REFCOUNTED_TYPE(TFakeCompanionClient);
DEFINE_REFCOUNTED_TYPE(TFakeCompanionClient);

////////////////////////////////////////////////////////////////////////////////

//! Process manager that spawns nothing and records its shutdown.
class TRecordingProcessManager
    : public TProcessManagerBase
{
public:
    explicit TRecordingProcessManager(const IInvokerPtr& invoker)
        : TProcessManagerBase(
            invoker,
            /*companionClient*/ nullptr,
            TExponentialBackoffOptions{},
            /*restartDelay*/ TDuration::Hours(1),
            /*healthCheckInterval*/ TDuration::Hours(1),
            /*startupGracePeriod*/ TDuration::Hours(1),
            /*metricsCollectionInterval*/ TDuration::Hours(1),
            NLogging::TLogger("RecordingProcessManager"),
            NProfiling::TProfiler(),
            CreateSyncStatusProfiler())
    { }

    bool IsShutDown() const
    {
        return ShutDown_.load();
    }

    void Shutdown() override
    {
        ShutDown_.store(true);
    }

protected:
    void ValidateParameters() const override
    { }

    TIntrusivePtr<TProcessBase> CreateProcessIncarnation() override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<void> HealthCheck() override
    {
        return MakeFuture(TError());
    }

private:
    std::atomic<bool> ShutDown_ = false;
};

DECLARE_REFCOUNTED_TYPE(TRecordingProcessManager);
DEFINE_REFCOUNTED_TYPE(TRecordingProcessManager);

////////////////////////////////////////////////////////////////////////////////

//! Manager whose own companion client is a scripted fake, so a reconcile pass
//! needs no live companion.
class TTestCompanionManager
    : public TCompanionManager
{
public:
    using TCompanionManager::ReconcileJobs;
    using TCompanionManager::TCompanionManager;

    void SetManagerClient(ICompanionClientPtr client)
    {
        CompanionClient_ = std::move(client);
    }

    //! Installs the process manager the destructor must shut down.
    TRecordingProcessManagerPtr AttachRecordingProcessManager()
    {
        auto processManager = New<TRecordingProcessManager>(GetContext()->Invoker);
        ProcessManager_ = processManager;
        return processManager;
    }
};

DECLARE_REFCOUNTED_TYPE(TTestCompanionManager);
DEFINE_REFCOUNTED_TYPE(TTestCompanionManager);

////////////////////////////////////////////////////////////////////////////////

class TJobRemovalTest
    : public ::testing::Test
{
protected:
    NConcurrency::TActionQueuePtr ActionQueue_ = New<NConcurrency::TActionQueue>("JobRemovalTest");

    TTestCompanionManagerPtr Manager_;
    TFakeCompanionClientPtr ManagerClient_ = New<TFakeCompanionClient>(/*processId*/ 1);

    void SetUp() override
    {
        auto context = New<TResourceContext>();
        context->ResourceId = TResourceId("companion_manager");
        context->ResourceInstanceId = TResourceInstanceId(TGuid::Create());
        context->ResourceSpec = ConvertTo<TResourceSpecPtr>(TYsonString(TStringBuf(R""""(
            {
                resource_class_name="NYT::NFlow::NCompanion::TCompanionManager";
                parameters={};
            }
        )"""")));
        context->Invoker = ActionQueue_->GetInvoker();
        context->Logger = NLogging::TLogger("TestCompanionManager");
        context->StatusProfiler = CreateSyncStatusProfiler();

        auto dynamicContext = New<TDynamicResourceContext>();
        dynamicContext->DynamicResourceSpec = ConvertTo<TDynamicResourceSpecPtr>(
            TYsonString(TStringBuf("{parameters={}}")));

        Manager_ = New<TTestCompanionManager>(std::move(context), std::move(dynamicContext));
        Manager_->SetManagerClient(ManagerClient_);
    }

    //! Waits until everything already posted to the manager invoker has run.
    void FlushInvoker()
    {
        auto barrier = BIND([] {
        }).AsyncVia(ActionQueue_->GetInvoker())
            .Run();
        NConcurrency::WaitFor(barrier)
            .ThrowOnError();
    }

    void Reconcile()
    {
        Manager_->ReconcileJobs();
        FlushInvoker();
    }
};

TEST_F(TJobRemovalTest, UnregisterSendsOnePromptRemoval)
{
    auto client = New<TFakeCompanionClient>(/*processId*/ 1);
    auto jobId = TJobId(TGuid::Create());
    client->HoldJob(jobId);

    Manager_->RegisterLiveJob(jobId, client);
    EXPECT_EQ(client->GetRemovalCount(), 0);

    Manager_->UnregisterLiveJob(jobId, client);
    EXPECT_EQ(client->GetRemovalCount(), 1);
    EXPECT_FALSE(client->HoldsJob(jobId));

    // The prompt removal succeeded: reconcile finds no orphans.
    Reconcile();
    EXPECT_EQ(client->GetRemovalCount(), 1);
}

TEST_F(TJobRemovalTest, LostRemovalIsRepairedByReconcile)
{
    auto jobId = TJobId(TGuid::Create());
    ManagerClient_->HoldJob(jobId);
    Manager_->RegisterLiveJob(jobId, ManagerClient_);

    // The prompt removal fails; the entry stays in the companion.
    ManagerClient_->FailNextRemovals(1);
    Manager_->UnregisterLiveJob(jobId, ManagerClient_);
    EXPECT_EQ(ManagerClient_->GetRemovalCount(), 1);
    EXPECT_TRUE(ManagerClient_->HoldsJob(jobId));

    Reconcile();
    EXPECT_FALSE(ManagerClient_->HoldsJob(jobId));
    EXPECT_EQ(ManagerClient_->GetRemovalCount(), 2);
}

TEST_F(TJobRemovalTest, ReconcileDoesNotRemoveALiveJob)
{
    auto jobId = TJobId(TGuid::Create());
    ManagerClient_->HoldJob(jobId);
    Manager_->RegisterLiveJob(jobId, ManagerClient_);

    Reconcile();
    Reconcile();
    EXPECT_EQ(ManagerClient_->GetRemovalCount(), 0);
    EXPECT_TRUE(ManagerClient_->HoldsJob(jobId));
}

TEST_F(TJobRemovalTest, ReconcileRunsWithZeroLiveJobs)
{
    // The manager's own client participates even when no live-job clients
    // exist: a drained worker is exactly when abandoned registrations linger.
    auto orphanId = TJobId(TGuid::Create());
    ManagerClient_->HoldJob(orphanId);

    Reconcile();
    EXPECT_FALSE(ManagerClient_->HoldsJob(orphanId));
    EXPECT_EQ(ManagerClient_->GetRemovedJobIds(), std::vector{orphanId});
}

TEST_F(TJobRemovalTest, SnapshotIsTakenAfterTheReply)
{
    // The safety proof of the reconcile pass: a job that joins the live set
    // while a ListJobs reply is in flight must not be treated as an orphan,
    // even when the reply already contains it. An implementation that
    // snapshots the live set when the request is sent would remove it.
    auto jobId = TJobId(TGuid::Create());

    // The job is registered at the companion before the pass, so the delayed
    // reply lists it.
    ManagerClient_->HoldJob(jobId);
    ManagerClient_->DelayNextListReply();
    Manager_->ReconcileJobs();
    FlushInvoker();

    // The live registration lands while the reply is in flight.
    Manager_->RegisterLiveJob(jobId, ManagerClient_);
    ManagerClient_->ReleaseListReply();
    FlushInvoker();

    EXPECT_EQ(ManagerClient_->GetRemovalCount(), 0);
    EXPECT_TRUE(ManagerClient_->HoldsJob(jobId));
}

TEST_F(TJobRemovalTest, ReconcileQueriesOneClientPerProcess)
{
    // Two jobs served by the same companion process: after the first pass
    // identifies both clients, each later pass queries one of them.
    auto firstClient = New<TFakeCompanionClient>(/*processId*/ 1);
    auto secondClient = New<TFakeCompanionClient>(/*processId*/ 1);
    auto firstJobId = TJobId(TGuid::Create());
    auto secondJobId = TJobId(TGuid::Create());
    firstClient->HoldJob(firstJobId);
    secondClient->HoldJob(secondJobId);

    Manager_->RegisterLiveJob(firstJobId, firstClient);
    Manager_->RegisterLiveJob(secondJobId, secondClient);

    // First pass: both clients still unidentified, so both are queried.
    Reconcile();
    EXPECT_EQ(firstClient->GetListCount(), 1);
    EXPECT_EQ(secondClient->GetListCount(), 1);

    // Later passes: one rotating representative for the identified group.
    Reconcile();
    EXPECT_EQ(ManagerClient_->GetListCount(), 2);
    EXPECT_EQ(firstClient->GetListCount() + secondClient->GetListCount(), 3);
    Reconcile();
    EXPECT_EQ(firstClient->GetListCount() + secondClient->GetListCount(), 4);
}

TEST_F(TJobRemovalTest, StaleRecordedPidIsEventuallyCorrected)
{
    // Two clients recorded against the same pid, then one silently migrates
    // to another fan-out child holding an orphan. The rotating representative
    // must reach the migrated client, correct its recorded pid and reclaim
    // the orphan; a fixed representative would shadow it forever.
    auto firstClient = New<TFakeCompanionClient>(/*processId*/ 1);
    auto secondClient = New<TFakeCompanionClient>(/*processId*/ 1);
    auto firstJobId = TJobId(TGuid::Create());
    auto secondJobId = TJobId(TGuid::Create());

    Manager_->RegisterLiveJob(firstJobId, firstClient);
    Manager_->RegisterLiveJob(secondJobId, secondClient);
    // Both clients get recorded with pid 1.
    Reconcile();

    auto orphanId = TJobId(TGuid::Create());
    secondClient->SetProcessId(2);
    secondClient->HoldJob(orphanId);

    // Within one full rotation over the group the orphan is reclaimed.
    Reconcile();
    Reconcile();
    EXPECT_FALSE(secondClient->HoldsJob(orphanId));
}

TEST_F(TJobRemovalTest, StaleDestructorDoesNotUnregisterTheNewIncarnation)
{
    // A job id can be re-registered by a newer computation incarnation
    // before the previous incarnation's destructor runs (asynchronous job
    // teardown). The stale unregister must neither drop the new live entry
    // nor remove the job from the companion.
    auto oldClient = New<TFakeCompanionClient>(/*processId*/ 1);
    auto newClient = New<TFakeCompanionClient>(/*processId*/ 1);
    auto jobId = TJobId(TGuid::Create());
    newClient->HoldJob(jobId);

    Manager_->RegisterLiveJob(jobId, oldClient);
    // The replacement incarnation registers before the old destructor runs.
    Manager_->RegisterLiveJob(jobId, newClient);
    Manager_->UnregisterLiveJob(jobId, oldClient);

    EXPECT_EQ(oldClient->GetRemovalCount(), 0);
    EXPECT_EQ(newClient->GetRemovalCount(), 0);

    // The job stays live through reconcile passes.
    Reconcile();
    EXPECT_TRUE(newClient->HoldsJob(jobId));

    Manager_->UnregisterLiveJob(jobId, newClient);
    EXPECT_FALSE(newClient->HoldsJob(jobId));
}

TEST_F(TJobRemovalTest, FanOutChildIsQueriedThroughItsOwnClient)
{
    // A job whose channel reaches a different process than the manager's
    // client keeps being queried through its own client.
    auto childClient = New<TFakeCompanionClient>(/*processId*/ 2);
    auto liveJobId = TJobId(TGuid::Create());
    auto orphanId = TJobId(TGuid::Create());
    childClient->HoldJob(liveJobId);
    childClient->HoldJob(orphanId);

    Manager_->RegisterLiveJob(liveJobId, childClient);

    Reconcile();
    EXPECT_FALSE(childClient->HoldsJob(orphanId));
    EXPECT_TRUE(childClient->HoldsJob(liveJobId));

    Reconcile();
    EXPECT_EQ(childClient->GetListCount(), 2);
}

TEST_F(TJobRemovalTest, RetainedRouteReconcilesAnUnreachableChild)
{
    // The unregistered job's client may be the only channel reaching its
    // fan-out child. After a failed prompt removal the entry must survive as
    // a removal route, so the reconcile pass can still query that child and
    // reclaim the job; erasing the client would strand the registration.
    auto childClient = New<TFakeCompanionClient>(/*processId*/ 2);
    auto jobId = TJobId(TGuid::Create());
    childClient->HoldJob(jobId);

    Manager_->RegisterLiveJob(jobId, childClient);
    childClient->FailNextRemovals(1);
    Manager_->UnregisterLiveJob(jobId, childClient);
    EXPECT_TRUE(childClient->HoldsJob(jobId));

    // The manager client reaches a different process; only the retained
    // route can reclaim the job.
    Reconcile();
    EXPECT_FALSE(childClient->HoldsJob(jobId));

    // Once its process no longer lists the job, the route is dropped and
    // stops being queried.
    Reconcile();
    auto listCount = childClient->GetListCount();
    Reconcile();
    EXPECT_EQ(childClient->GetListCount(), listCount);
}

TEST_F(TJobRemovalTest, StaleCopyOfALiveJobIsRemoved)
{
    // A live job whose channel migrated leaves a stale copy behind in the
    // old process. The copy answers RS_OK if the channel ever migrates back,
    // so it must be removed even though its id is globally live; only the
    // copy in the process the job's own channel reaches survives.
    auto jobClient = New<TFakeCompanionClient>(/*processId*/ 2);
    auto jobId = TJobId(TGuid::Create());
    jobClient->HoldJob(jobId);
    Manager_->RegisterLiveJob(jobId, jobClient);

    // First pass records the job's channel at process 2.
    Reconcile();
    EXPECT_TRUE(jobClient->HoldsJob(jobId));

    // A stale copy surfaces in the manager client's process.
    ManagerClient_->HoldJob(jobId);
    Reconcile();
    EXPECT_FALSE(ManagerClient_->HoldsJob(jobId));
    EXPECT_TRUE(jobClient->HoldsJob(jobId));
}

TEST_F(TJobRemovalTest, DestructorStopsTheCompanionProcess)
{
    auto processManager = Manager_->AttachRecordingProcessManager();
    EXPECT_FALSE(processManager->IsShutDown());

    // A destroyed manager must stop the companion process.
    Manager_.Reset();
    EXPECT_TRUE(processManager->IsShutDown());
}

TEST_F(TJobRemovalTest, UnregisterDoesNotWaitForTheCompanion)
{
    auto client = New<TFakeCompanionClient>(/*processId*/ 1);
    client->HangRemovals();
    auto jobId = TJobId(TGuid::Create());
    Manager_->RegisterLiveJob(jobId, client);

    // A destructor must not block behind a companion that never answers.
    auto startedAt = TInstant::Now();
    Manager_->UnregisterLiveJob(jobId, client);
    EXPECT_LT(TInstant::Now() - startedAt, TDuration::Seconds(5));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanion
