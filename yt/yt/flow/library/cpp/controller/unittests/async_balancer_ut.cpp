#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/controller/job_manager.h>

#include <yt/yt/flow/library/cpp/computation/universal_controller.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NFlow {

using namespace NController;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

//! Fake storage handler. Enough for this test since there's no recovery after restart.
class TStorageHandler : public TPersistedStateStorageHandlerBase<std::string>
{
public:
    using TStorageRow = typename TPersistedStateStorageHandlerBase<std::string>::TStorageRow;

    void Select(TSequenceId, std::vector<TStorageRow>&) override
    { }

    void Execute(std::vector<TStorageRow>&&, const std::vector<TSequenceId>&, bool, const std::vector<TPersistedStateCommitContext*>&) override
    { }
};

//! Exercises the async CpuAware balancer path (AsyncBalancing = true), which the rest of
//! the controller unit tests skip (they force AsyncBalancing = false).
//!
//! The balancer runs on a dedicated background invoker; the scheduling steps run on the
//! current (main-cycle) invoker, exactly as in production.
class TAsyncBalancer
    : public ::testing::Test
{
public:
    TIntrusivePtr<TStorageHandler> StorageHandler = New<TStorageHandler>();
    TPersistedStateControlPtr<std::string> PersistedControl;
    TFlowViewPtr FlowView;
    IJobManagerPtr JobManager;
    TPipelineSpecPtr Spec;
    TDynamicPipelineSpecPtr DynamicSpec;
    TActionQueuePtr BalancerQueue;
    std::vector<TComputationId> Computations;

    TComputationSpecPtr MakeComputationSpec(const TComputationId& id)
    {
        auto spec = New<TComputationSpec>();
        spec->ComputationClassName = "NYT::NFlow::TPassthroughComputation";
        spec->GroupBySchema = New<NTableClient::TTableSchema>(std::vector<NTableClient::TColumnSchema>{
            NTableClient::TColumnSchema("hash", NTableClient::EValueType::Uint64).SetRequired(true)});
        spec->InputStreamIds.insert(TStreamId(Format("%v_input", id)));
        spec->OutputStreamIds.insert(TStreamId(Format("%v_output", id)));
        spec->TimerStreams[TStreamId(Format("%v_timer", id))] = New<TTimerSpec>();
        return spec;
    }

    TStreamSpecPtr MakeStreamSpec()
    {
        auto streamSpec = New<TStreamSpec>();
        streamSpec->ClassName = "FakeClassName";
        streamSpec->Schema = ConvertTo<NTableClient::TTableSchemaPtr>(NYson::TYsonString(TStringBuf(R""""(
            [{name="value"; type="string";};]
        )"""")));
        return streamSpec;
    }

    //! Builds Spec/DynamicSpec for the given computations and pushes them onto the flow view.
    void BuildSpecs(const std::vector<TComputationId>& computations)
    {
        Computations = computations;
        Spec = New<TPipelineSpec>();
        DynamicSpec = New<TDynamicPipelineSpec>();

        // The whole point of this fixture: exercise the async balancer.
        DynamicSpec->JobManager->AsyncBalancing = true;
        DynamicSpec->JobManager->BalancerType = EJobBalancerType::CpuAware;
        // Production defaults make one background balancing cycle take ~10s
        // (RebalanceSyncPeriod) with a 4s per-computation min time. Shrink them so the
        // background balancer reacts within the test's pump loop.
        DynamicSpec->JobManager->RebalanceSyncPeriod = TDuration::MilliSeconds(50);
        DynamicSpec->JobManager->RebalanceActionMinTime = TDuration::Zero();
        DynamicSpec->JobManager->RebalanceActionMaxTime = TDuration::MilliSeconds(50);
        DynamicSpec->JobManager->RebalanceDelayAfterPipelineSync = TDuration::Zero();

        for (const auto& id : computations) {
            Spec->Computations[id] = MakeComputationSpec(id);
            Spec->Streams[TStreamId(Format("%v_output", id))] = MakeStreamSpec();

            DynamicSpec->Computations[id] = New<TDynamicComputationSpec>();
            DynamicSpec->Computations[id]->Parameters = ConvertTo<IMapNodePtr>(
                TYsonString(TStringBuf(R""""(
                    {
                        "partition_count_double_delay" = 0;
                        "partition_count_half_delay" = 0;
                    }
                )"""")));
        }

        FlowView->State->ExecutionSpec->PipelineSpec->SetValue(Spec);
        FlowView->State->ExecutionSpec->DynamicPipelineSpec->SetValue(DynamicSpec);
        FlowView->State->ExecutionSpec->ExtendedPipelineSpec->SetValue(BuildExtendedPipelineSpec(Spec));
        FlowView->State->ExecutionSpec->PipelineState->SetValue(EPipelineState::Working);
        FlowView->CurrentSpec->SetValue(Spec);
    }

    //! (Re)creates the job manager for the current Spec/DynamicSpec, starting a fresh async
    //! balancer. Stops the previous one if any. Recreating with fewer computations is how we
    //! simulate "a computation was removed from the spec" — the layout still holds its
    //! partitions but there is no controller for them anymore.
    void RecreateJobManager()
    {
        StopJobManager();

        BalancerQueue = New<TActionQueue>("Balancer");

        auto context = New<TJobManagerContext>();
        context->Invoker = BalancerQueue->GetInvoker();
        context->MainCycleInvoker = GetCurrentInvoker();
        context->PipelinePath = NYPath::TRichYPath::Parse("<cluster=pipeline_cluster>//pipeline/path");
        context->StatusProfiler = CreateSyncStatusProfiler();
        JobManager = NController::CreateJobManager(context, Spec, DynamicSpec, FlowView->State->JobManagerState, /*authenticator*/ nullptr);
    }

    void StopJobManager()
    {
        if (JobManager) {
            // Stop the background balancer thread before tearing down its queue.
            DynamicSpec->JobManager->AsyncBalancing = false;
            JobManager->Reconfigure(DynamicSpec);
            DynamicSpec->JobManager->AsyncBalancing = true;
            JobManager = nullptr;
        }
        if (BalancerQueue) {
            BalancerQueue->Shutdown();
            BalancerQueue.Reset();
        }
    }

    void AddWorkers(ssize_t numWorkers)
    {
        FlowView->State->StartMutation();
        for (ssize_t i = 0; i < numWorkers; i++) {
            auto worker = New<NFlow::TWorker>();
            worker->RpcAddress = Format("worker-%v.net:81", i);
            worker->MonitoringAddress = Format("worker-%v.net:80", i);
            worker->IncarnationId = TIncarnationId(TGuid::Create());
            FlowView->State->Workers[worker->RpcAddress] = worker;
        }
        FlowView->State->CommitMutation();
    }

    void Prepare(const std::vector<TComputationId>& computations, ssize_t numWorkers)
    {
        BuildSpecs(computations);
        RecreateJobManager();
        AddWorkers(numWorkers);
    }

    //! One full scheduling iteration, mirroring the controller's manageJobs sequence.
    void Schedule()
    {
        FlowView->State->StartMutation();
        JobManager->RemoveFailedJobs(FlowView);
        JobManager->RemoveLostJobs(FlowView);
        JobManager->DoPartitioning(FlowView);
        JobManager->DistributeJobs(FlowView);
        FlowView->State->CommitMutation();
    }

    //! Like Schedule(), but additionally performs a transient layout mutation (create +
    //! immediately remove a throwaway partition) every iteration — exactly the way the
    //! per-computation DoPartitioning churns partitions on a busy pipeline.
    void ScheduleWithPartitionUpdates()
    {
        FlowView->State->StartMutation();
        JobManager->RemoveFailedJobs(FlowView);
        JobManager->RemoveLostJobs(FlowView);
        JobManager->DoPartitioning(FlowView);
        {
            const auto& layout = Layout();
            auto partition = New<TPartition>();
            partition->PartitionId = TPartitionId(TPartitionId::TUnderlying::Create());
            partition->ComputationId = Computations.front();
            partition->State = EPartitionState::Executing;
            partition->StateEpoch = FlowView->State->ExecutionSpec->GetEpoch();
            partition->StateTimestamp = TInstant::Now();
            layout->CreatePartition(partition);
            layout->RemovePartition(partition->PartitionId);
        }
        JobManager->DistributeJobs(FlowView);
        FlowView->State->CommitMutation();
    }

    //! Puts a (varied-by-partition) CPU usage on every placed partition's feedback status, so the
    //! balancer sees a real per-worker CPU deviation and actually enters slow balancing.
    void InjectCpu()
    {
        // Production publishes a freshly built feedback snapshot every tick (CollectFeedback +
        // SetFeedback) and never mutates an already-published one, which the balancer fiber may
        // still be reading. Mirror that here by republishing a fresh clone before mutating —
        // otherwise the writes below race with the fiber's reads under TSan.
        FlowView->Feedback = CloneYsonStruct(FlowView->Feedback);
        for (const auto& [partitionId, partition] : Layout()->Partitions) {
            if (!partition->CurrentJobId) {
                continue;
            }
            auto& status = FlowView->Feedback->PartitionJobStatuses[partitionId];
            if (!status) {
                status = New<TPartitionJobStatus>();
            }
            // Keep an existing failed status (so RemoveFailedJobs still fires); otherwise build an
            // OK status that only carries CPU.
            if (!status->CurrentJobStatus || status->CurrentJobStatus->Error.IsOK()) {
                auto jobStatus = New<TJobStatus>();
                jobStatus->JobId = *partition->CurrentJobId;
                jobStatus->StartTime = TInstant::Now() - TDuration::Minutes(1);
                status->CurrentJobId = *partition->CurrentJobId;
                status->CurrentJobStatus = jobStatus;
            }
            status->CurrentJobStatusUpdateTime = TInstant::Now();
            double varied = 1.0 + static_cast<double>(THash<TPartitionId>()(partitionId) % 1000) / 50.0;
            status->CurrentJobStatus->PerformanceMetrics->CpuUsage10m = varied;
        }
    }

    //! Like InjectCpu, but puts high CpuUsage on the partitions of one "hot" worker and low on the
    //! rest — a strong, stable per-worker imbalance so the balancer enters slow balancing
    //! (RelieveWorker) and buffers deferred "move off the hot worker" actions.
    void InjectImbalancedCpu(const std::string& hotWorker)
    {
        FlowView->Feedback = CloneYsonStruct(FlowView->Feedback);
        for (const auto& [partitionId, partition] : Layout()->Partitions) {
            if (!partition->CurrentJobId) {
                continue;
            }
            auto& status = FlowView->Feedback->PartitionJobStatuses[partitionId];
            if (!status) {
                status = New<TPartitionJobStatus>();
            }
            if (!status->CurrentJobStatus || status->CurrentJobStatus->Error.IsOK()) {
                auto jobStatus = New<TJobStatus>();
                jobStatus->JobId = *partition->CurrentJobId;
                jobStatus->StartTime = TInstant::Now() - TDuration::Minutes(1);
                status->CurrentJobId = *partition->CurrentJobId;
                status->CurrentJobStatus = jobStatus;
            }
            status->CurrentJobStatusUpdateTime = TInstant::Now();
            const auto& job = Layout()->Jobs.at(*partition->CurrentJobId);
            status->CurrentJobStatus->PerformanceMetrics->CpuUsage10m = (job->WorkerAddress == hotWorker) ? 50.0 : 0.5;
        }
    }

    //! Resets to a fresh flow view + job manager so the scenario can be repeated from scratch.
    void ResetState()
    {
        StopJobManager();
        FlowView = New<TFlowView>();
        PersistedControl = New<TPersistedStateControl<std::string>>(StorageHandler);
        FlowView->State->AttachToControl(PersistedControl);
        PersistedControl->Recover();
        JobManager = nullptr;
    }

    //! Runs one instance of the markov/limbert incident scenario and checks the one-job-per-
    //! partition invariant. The test repeats it because the failure is timing-sensitive.
    void RunGracefulFailScenario(int iteration)
    {
        ResetState();
        Prepare(/*computations*/ {"computation"}, /*numWorkers*/ 4);

        // Reach a healthy, fully-placed steady state first (plain schedule, like production
        // before the incident).
        PumpUntilAllPlacedOrBudget(/*maxIterations*/ 50);
        ASSERT_GT(PlacedJobCount(), 0u) << "iteration " << iteration;
        ASSERT_EQ(PlacedJobCount(), ExecutingPartitionCount()) << "iteration " << iteration;

        // Mark up to 3 placed partitions as "graceful move pending to another worker", then fail
        // their jobs (non-retryable) — the dangling pending is NOT cleared on failure.
        // Republish fresh feedback/ephemeral snapshots first (see InjectCpu): mutating the
        // currently published ones in place would race with the balancer fiber's reads.
        FlowView->Feedback = CloneYsonStruct(FlowView->Feedback);
        FlowView->EphemeralState = CloneYsonStruct(FlowView->EphemeralState);
        int victims = 0;
        for (const auto& [partitionId, partition] : Layout()->Partitions) {
            if (victims >= 3 || !partition->CurrentJobId) {
                continue;
            }
            auto job = Layout()->Jobs.at(*partition->CurrentJobId);
            std::string target;
            for (const auto& [address, _] : FlowView->State->Workers) {
                if (address != job->WorkerAddress) {
                    target = address;
                    break;
                }
            }
            FlowView->EphemeralState->GetPartitionState(partitionId)->PendingGracefulRebalanceWorkerAddress = target;

            auto& status = FlowView->Feedback->PartitionJobStatuses[partitionId];
            if (!status) {
                status = New<TPartitionJobStatus>();
            }
            auto jobStatus = New<TJobStatus>();
            jobStatus->JobId = *partition->CurrentJobId;
            jobStatus->IsFinished = true;
            jobStatus->Error = TError("Commit attempt failed, error is not retryable");
            status->CurrentJobId = *partition->CurrentJobId;
            status->CurrentJobStatus = jobStatus;
            status->CurrentJobStatusUpdateTime = TInstant::Now();
            ++victims;
        }
        ASSERT_EQ(victims, 3) << "iteration " << iteration;

        // Keep the pipeline busy (CPU imbalance + partition updates, unchanged spec) so the
        // balancer enters slow balancing on the mid-mutation (inconsistent) snapshot. Wait until
        // every partition owns a job: a dead/looping balancer leaves the victims permanently
        // jobless, so this never converges and the assertions below fire.
        for (int i = 0; i < 80; ++i) {
            InjectCpu();
            ScheduleWithPartitionUpdates();
            Sleep(TDuration::MilliSeconds(100));
            if (JoblessPartitionCount() == 0) {
                break;
            }
        }

        // Every partition must carry exactly one job, and every job must belong to an existing
        // partition that points back to it. A balancer that died on the inconsistent snapshot
        // leaves the failed victims jobless; a corrupted balancer leaves stray jobs. (Partitions
        // mid graceful-move sit in Interrupting but still legitimately own their job, so we count
        // all partitions, not just the Executing ones.)
        EXPECT_EQ(JoblessPartitionCount(), 0u)
            << "iteration " << iteration << ": " << JoblessPartitionCount()
            << " partitions left jobless after graceful-pending jobs failed";
        EXPECT_EQ(PlacedJobCount(), Layout()->Partitions.size())
            << "iteration " << iteration << ": " << PlacedJobCount() << " jobs for "
            << Layout()->Partitions.size() << " partitions (stray or missing jobs)";
        for (const auto& [jobId, job] : Layout()->Jobs) {
            auto it = Layout()->Partitions.find(job->PartitionId);
            EXPECT_TRUE(it != Layout()->Partitions.end() && it->second->CurrentJobId == jobId)
                << "iteration " << iteration << ": stray job " << ToString(jobId);
            EXPECT_TRUE(FlowView->State->Workers.contains(job->WorkerAddress)) << "iteration " << iteration;
        }
    }

    void Pump(int iterations)
    {
        for (int i = 0; i < iterations; ++i) {
            Schedule();
            Sleep(TDuration::MilliSeconds(100));
        }
    }

    void PumpUntilAllPlacedOrBudget(int maxIterations)
    {
        for (int i = 0; i < maxIterations; ++i) {
            Schedule();
            Sleep(TDuration::MilliSeconds(100));
            if (ExecutingPartitionCount() > 0 && PlacedJobCount() >= ExecutingPartitionCount()) {
                break;
            }
        }
    }

    const TFlowLayoutPtr& Layout() const
    {
        return FlowView->State->ExecutionSpec->Layout;
    }

    size_t ExecutingPartitionCount() const
    {
        size_t count = 0;
        for (const auto& [_, partition] : Layout()->Partitions) {
            if (partition->State == EPartitionState::Executing) {
                ++count;
            }
        }
        return count;
    }

    size_t JoblessPartitionCount() const
    {
        size_t count = 0;
        for (const auto& [_, partition] : Layout()->Partitions) {
            if (!partition->CurrentJobId) {
                ++count;
            }
        }
        return count;
    }

    size_t PartitionCountForComputation(const TComputationId& id) const
    {
        size_t count = 0;
        for (const auto& [_, partition] : Layout()->Partitions) {
            if (partition->ComputationId == id) {
                ++count;
            }
        }
        return count;
    }

    size_t PlacedJobCount() const
    {
        return Layout()->Jobs.size();
    }

    void SetUp() override
    {
        FlowView = New<TFlowView>();
        PersistedControl = New<TPersistedStateControl<std::string>>(StorageHandler);
        FlowView->State->AttachToControl(PersistedControl);
        PersistedControl->Recover();
        JobManager = nullptr;
    }

    void TearDown() override
    {
        StopJobManager();
    }
};

////////////////////////////////////////////////////////////////////////////////

//! 1) Baseline: with the async balancer, all stray partitions eventually get a job.
TEST_F(TAsyncBalancer, PlacesStrayPartitions)
{
    Prepare(/*computations*/ {"computation"}, /*numWorkers*/ 4);

    PumpUntilAllPlacedOrBudget(/*maxIterations*/ 50);

    EXPECT_GT(ExecutingPartitionCount(), 0u);
    EXPECT_EQ(PlacedJobCount(), ExecutingPartitionCount());
}

//! 2) Dangerous change: the pipeline restarts with a new spec from which a computation has
//! been removed, while the layout still holds that computation's partitions. The balancer
//! must not be fed a layout it cannot reconcile against the spec. Since the pushed snapshot
//! now reflects the in-flight mutation (YTFLOW-625), it carries the DoPartitioning that
//! interrupts the orphaned partitions, so it stays consistent with the new spec. Expected
//! behaviour: the controller interrupts the orphaned partitions and does NOT crash; it is
//! fine that they are not balanced.
TEST_F(TAsyncBalancer, DangerousSpecChangeDoesNotCrash)
{
    Prepare(/*computations*/ {"kept", "removed"}, /*numWorkers*/ 4);

    // Bring both computations up and placed.
    PumpUntilAllPlacedOrBudget(/*maxIterations*/ 50);
    ASSERT_GT(PartitionCountForComputation("removed"), 0u);
    ASSERT_EQ(PlacedJobCount(), ExecutingPartitionCount());

    // Restart with a spec that no longer contains the "removed" computation. Its partitions
    // are still in the layout but there is no controller for them anymore. In production a
    // spec change recreates the flow view / restarts the controller, so stop the previous
    // balancer before swapping the spec (otherwise it races on the shared CurrentSpec).
    StopJobManager();
    BuildSpecs(/*computations*/ {"kept"});
    RecreateJobManager();

    // Run EXACTLY ONE scheduling iteration, then let the freshly started background balancer
    // react to it. In this iteration DoPartitioning interrupts the orphaned "removed"
    // partitions. The push happens within that same mutation and snapshots its uncommitted
    // state (CreateSnapshot(committed=false)), so the balancer sees the partitions already
    // interrupted/reconciled against the new spec rather than the stale committed layout that
    // still had "removed" as Executing — and does not crash in CollectPartitions.
    Schedule();
    Sleep(TDuration::Seconds(2));

    // Reaching this point means the process did not crash. The orphaned computation's
    // partitions are interrupted; the kept one keeps running.
    for (const auto& [_, partition] : Layout()->Partitions) {
        if (partition->ComputationId == TComputationId("removed")) {
            EXPECT_NE(partition->State, EPartitionState::Executing);
        }
    }
    EXPECT_GT(PartitionCountForComputation("kept"), 0u);
}

//! 3) Harmless change: a worker disappears, so its jobs are lost and its partitions become
//! stray. Balancing must keep working and re-place them on the remaining workers.
TEST_F(TAsyncBalancer, HarmlessWorkerLossIsRebalanced)
{
    Prepare(/*computations*/ {"computation"}, /*numWorkers*/ 4);

    PumpUntilAllPlacedOrBudget(/*maxIterations*/ 50);
    ASSERT_EQ(PlacedJobCount(), ExecutingPartitionCount());
    ASSERT_GT(PlacedJobCount(), 0u);

    // A worker vanishes. Its jobs are now hosted by a non-existent worker.
    {
        FlowView->State->StartMutation();
        auto victim = FlowView->State->Workers.begin()->first;
        FlowView->State->Workers.erase(victim);
        FlowView->State->CommitMutation();
    }

    // RemoveLostJobs orphans the affected partitions; the balancer must re-place them.
    PumpUntilAllPlacedOrBudget(/*maxIterations*/ 50);

    EXPECT_EQ(PlacedJobCount(), ExecutingPartitionCount());
    // Every job must sit on a worker that still exists.
    for (const auto& [_, job] : Layout()->Jobs) {
        EXPECT_TRUE(FlowView->State->Workers.contains(job->WorkerAddress));
    }
}

//! 4) Models the YTFLOW-616 incident: a healthy steady state, then a harmless change (a worker
//! disappears) that needs re-balancing, while the pipeline keeps churning partitions (unchanged
//! spec). A historical push gate keyed on a partition-updated counter permanently deferred the
//! async balancer Push under such churn, starving the orphaned partitions. The push is now
//! unconditional (the snapshot is always consistent, YTFLOW-625), so re-balancing must proceed.
TEST_F(TAsyncBalancer, RebalancesStrayPartitionsUnderContinuousPartitionUpdates)
{
    Prepare(/*computations*/ {"computation"}, /*numWorkers*/ 4);

    // Healthy steady state: everything placed, so the balancer has pushed at least once.
    PumpUntilAllPlacedOrBudget(/*maxIterations*/ 50);
    ASSERT_GT(PlacedJobCount(), 0u);
    ASSERT_EQ(PlacedJobCount(), ExecutingPartitionCount());

    // A worker vanishes: its partitions become stray. The spec does NOT change.
    {
        FlowView->State->StartMutation();
        auto victim = FlowView->State->Workers.begin()->first;
        FlowView->State->Workers.erase(victim);
        FlowView->State->CommitMutation();
    }

    // Continuous harmless partition updates while the orphaned partitions wait for a job.
    for (int i = 0; i < 50; ++i) {
        ScheduleWithPartitionUpdates();
        Sleep(TDuration::MilliSeconds(100));
        if (PlacedJobCount() >= ExecutingPartitionCount()) {
            break;
        }
    }

    EXPECT_EQ(PlacedJobCount(), ExecutingPartitionCount())
        << "async balancer starved by continuous partition updates (Push deferred): "
        << PlacedJobCount() << " of " << ExecutingPartitionCount() << " partitions placed";
    for (const auto& [_, job] : Layout()->Jobs) {
        EXPECT_TRUE(FlowView->State->Workers.contains(job->WorkerAddress));
    }
}

//! 5) Reproduces the markov/limbert incident (2026-06-10): three jobs were already chosen for a
//! graceful move (PendingGracefulRebalanceWorkerAddress set, waiting for the current job to
//! finish) when they instead failed non-retryably. RemoveFailedJobs drops the jobs and adjusts
//! the (live) flow view, but the weakened push gate (YTFLOW-616) feeds the async balancer a
//! snapshot cloned mid-mutation: TFlowState::Clone() keeps the live Workers/EphemeralState (with
//! the dangling pending-graceful targets) but rolls the layout back to the committed state where
//! the jobs are NOT yet deleted. The balancer's CollectPartitions then places those partitions
//! on their graceful TARGETS (live ephemeral) while the committed layout/verifier still see them
//! on the SOURCES — a divergent per-worker bookkeeping that, under slow balancing, the persisted
//! ActionsBuffer carries forward and trips on. In the incident the background balancer fiber
//! died on this and never recovered.
//!
//! Expected: the orphaned partitions are re-placed and the balancer keeps working. The fix
//! (YTFLOW-625) feeds the balancer a consistent snapshot (committed layout + the in-flight
//! mutation's uncommitted writes), so the removed jobs are visible and no inconsistent bookkeeping
//! is produced. Without the fix this aborts on YT_VERIFY in DelPartition (or leaves victims jobless).
TEST_F(TAsyncBalancer, ReplacesPartitionsAfterGracefulPendingJobsFail)
{
    // The failure was timing-sensitive (the background balancer fiber corrupted its bookkeeping on
    // a different iteration each run, sometimes hard-aborting on YT_VERIFY in DelPartition),
    // so repeat the whole scenario several times to make the regression reliable.
    for (int iteration = 0; iteration < 10; ++iteration) {
        RunGracefulFailScenario(iteration);
    }
}

//! 6) End-to-end reproduction of the DelPartition:1539 abort, driven entirely through the real async
//! balancer (no test seam). Two code paths compute where each partition is: the emulation seed
//! (CollectPartitions) and the action verifier (BuildPartitionLocations). Both must apply the
//! pending-graceful-rebalance redirect — a graceful-moving partition is treated as already on its
//! TARGET worker. If the verifier omits the redirect (the bug) it keeps the partition on its SOURCE;
//! a "Del from source" it then produces/keeps is replayed by RebalanceJobs into an emulation that
//! seeded the partition on the target, corrupting the per-partition action bookkeeping. The next
//! DelPartition aborts on YT_VERIFY (job_balancer.cpp:1539) via KickPartitionsFromOvercountedWorkers
//! (fast) or RelieveWorker (slow). With the redirect these actions are dropped and nothing aborts.
//!
//! The graceful-move state is produced by the balancer itself: GracefulMove=true makes a chosen move
//! set PendingGracefulRebalanceWorkerAddress and keep the job on its SOURCE until that job finishes.
//! Here the source jobs never finish (no real workers), so a strong, sustained CPU imbalance on one
//! "hot" worker makes the balancer keep choosing graceful moves whose source jobs pile up on the hot
//! worker — exactly the divergence (job on source, counted on target) that the verifier mishandles
//! without the fix. The failure is timing-sensitive, so the scenario is repeated several times.
TEST_F(TAsyncBalancer, GracefulPendingMoveAbortsOnDelPartition)
{
    for (int iteration = 0; iteration < 2; ++iteration) {
        ResetState();
        Prepare(/*computations*/ {"computation"}, /*numWorkers*/ 4);

        // Let the balancer initiate graceful moves itself, and apply them immediately (no stability
        // delay) so the pending-graceful state appears within the pump loop.
        DynamicSpec->JobManager->GracefulMove = true;
        DynamicSpec->JobManager->RebalanceDelayAfterPipelineSync = TDuration::Zero();
        // Pin a fixed, moderate partition count (no doubling under the high injected CPU), but enough
        // partitions per worker that the balancer can actually shed some off the hot worker.
        DynamicSpec->Computations["computation"]->Parameters = ConvertTo<IMapNodePtr>(
            TYsonString(TStringBuf(R""""(
            {
                "desired_partition_count" = 40;
                "partition_count_double_delay" = 3600000000;
                "partition_count_half_delay" = 3600000000;
            }
        )"""")));
        JobManager->Reconfigure(DynamicSpec);

        PumpUntilAllPlacedOrBudget(/*maxIterations*/ 50);
        ASSERT_GT(PlacedJobCount(), 0u) << "iteration " << iteration;

        // The hot worker hosts the jobs the balancer keeps trying to move off. Its graceful-move source
        // jobs never finish in the test, so the imbalance — and the pending-graceful divergence — keeps
        // growing each round. Without the BuildPartitionLocations redirect this aborts on DelPartition.
        const std::string hotWorker = FlowView->State->Workers.begin()->first;
        for (int i = 0; i < 80; ++i) {
            InjectImbalancedCpu(hotWorker);
            // Orphan one partition each tick so a stray is always present: this forces DoBalance to pull
            // (foundStrayPartitions) and actually apply the balancer's CPU-driven moves — which, with
            // GracefulMove=true, set PendingGracefulRebalanceWorkerAddress — without a synced pipeline.
            FlowView->State->StartMutation();
            for (const auto& [partitionId, partition] : Layout()->Partitions) {
                if (partition->CurrentJobId) {
                    Layout()->RemoveJob(*partition->CurrentJobId, EJobFinishReason::Rebalanced);
                    break;
                }
            }
            FlowView->State->CommitMutation();
            Schedule();
            Sleep(TDuration::MilliSeconds(50));
        }

        // Reaching here (with the fix) means the balancer kept its bookkeeping consistent: every job
        // belongs to an existing partition that points back to it, on an existing worker.
        for (const auto& [jobId, job] : Layout()->Jobs) {
            auto it = Layout()->Partitions.find(job->PartitionId);
            EXPECT_TRUE(it != Layout()->Partitions.end() && it->second->CurrentJobId == jobId)
                << "iteration " << iteration << ": stray job " << ToString(jobId);
            EXPECT_TRUE(FlowView->State->Workers.contains(job->WorkerAddress)) << "iteration " << iteration;
        }
    }
}

//! 7) The async balancer fiber reads EphemeralState (PendingGracefulRebalanceWorkerAddress in
//! BuildPartitionLocations / CollectPartitions) on its own thread, while the main cycle mutates that
//! same EphemeralState in place every tick (DistributeJobs::gracefulStopJob does exactly
//! GetPartitionState(pid)->PendingGracefulRebalanceWorkerAddress = target; DoPartitioning writes
//! LastOkTime). TFlowView::CopyPtr only shallow-copies the EphemeralState pointer, so before the fix
//! the pushed snapshot shared the live object: a data race (and an inconsistent State-frozen /
//! EphemeralState-live pair). Push now clones EphemeralState, freezing a consistent snapshot for the
//! fiber.
//!
//! This reproduces the race: with the balancer running, hammer EphemeralState in place on the main
//! thread (mirroring gracefulStopJob) between pushes. Under TSan, without the clone the fiber's read
//! of the shared EphemeralState races these writes; with the clone the fiber reads its own copy and
//! the run is clean.
TEST_F(TAsyncBalancer, PushClonesEphemeralStateForFiber)
{
    Prepare(/*computations*/ {"computation"}, /*numWorkers*/ 4);

    // Pin a fixed partition count so the injected CPU does not double partitions (which would make
    // the test run for minutes); a handful per worker is enough to exercise the fiber's reads.
    DynamicSpec->Computations["computation"]->Parameters = ConvertTo<IMapNodePtr>(
        TYsonString(TStringBuf(R""""(
            {
                "desired_partition_count" = 16;
                "partition_count_double_delay" = 3600000000;
                "partition_count_half_delay" = 3600000000;
            }
        )"""")));
    JobManager->Reconfigure(DynamicSpec);

    PumpUntilAllPlacedOrBudget(/*maxIterations*/ 50);
    ASSERT_GT(PlacedJobCount(), 0u);

    const std::string targetWorker = FlowView->State->Workers.begin()->first;

    // Pre-create an ephemeral state for every partition so the in-place hammering below only updates
    // existing values (as gracefulStopJob does) and never restructures the map while the fiber reads.
    for (const auto& [partitionId, partition] : Layout()->Partitions) {
        FlowView->EphemeralState->GetPartitionState(partitionId);
    }

    for (int i = 0; i < 50; ++i) {
        InjectCpu();
        Schedule();
        // The push above (without the fix) made StartData_ share this very EphemeralState. Mutate it
        // in place repeatedly while the fiber keeps reading it for the rest of this epoch.
        for (int j = 0; j < 20; ++j) {
            for (const auto& [partitionId, partition] : Layout()->Partitions) {
                FlowView->EphemeralState->GetPartitionState(partitionId)->PendingGracefulRebalanceWorkerAddress =
                    (j % 2 == 0) ? std::optional(targetWorker) : std::nullopt;
            }
            Sleep(TDuration::MilliSeconds(1));
        }
    }

    // The signal is TSan (data race without the clone). The process must also stay healthy: every job
    // belongs to an existing partition that points back to it.
    for (const auto& [jobId, job] : Layout()->Jobs) {
        auto it = Layout()->Partitions.find(job->PartitionId);
        EXPECT_TRUE(it != Layout()->Partitions.end() && it->second->CurrentJobId == jobId)
            << "stray job " << ToString(jobId);
    }
}

//! 8) rebalance_min_cpu_spread also gates the FAST (count-based) rebalancing. When the worker CPU
//! spread is below the threshold and there are no stray partitions, the overcount kick must be
//! skipped: an artificially count-overloaded worker is left as-is instead of being churned.
//!
//! Drives the balancer SYNCHRONOUSLY (AsyncBalancing=false) so DoFastBalancing's result is applied
//! directly and the gate's effect is deterministic — the async path only pulls actions once the
//! pipeline is "synced", a state this fixture never reaches. The threshold is set far above any real
//! spread, so the gate suppresses the kick whenever there are no strays.
TEST_F(TAsyncBalancer, FastRebalanceSuppressedWhenSpreadSmall)
{
    Prepare(/*computations*/ {"computation"}, /*numWorkers*/ 4);
    DynamicSpec->JobManager->AsyncBalancing = false;
    // Hard moves (not graceful) so a kicked partition leaves its worker immediately and the count
    // change is observable within Schedule() (a graceful move would keep the job on the source until
    // the epoch finishes, which this test never drives).
    DynamicSpec->JobManager->GracefulMove = false;
    DynamicSpec->Computations["computation"]->Parameters = ConvertTo<IMapNodePtr>(
        TYsonString(TStringBuf(R""""(
            {
                "desired_partition_count" = 40;
                "partition_count_double_delay" = 3600000000;
                "partition_count_half_delay" = 3600000000;
            }
        )"""")));
    DynamicSpec->JobManager->RebalanceMinCpuSpread = 1000000000.0;
    JobManager->Reconfigure(DynamicSpec);

    for (int i = 0; i < 20 && !(ExecutingPartitionCount() > 0 && PlacedJobCount() >= ExecutingPartitionCount()); ++i) {
        Schedule();
    }
    ASSERT_GT(PlacedJobCount(), 0u);
    ASSERT_EQ(PlacedJobCount(), ExecutingPartitionCount());

    // Create a pure count overload (no strays): move several jobs from one worker onto another so the
    // latter sits well above the per-worker count target. WITHOUT the gate the kick would offload it
    // back onto the now-undercounted worker.
    auto jobCountOn = [&] (const std::string& address) {
        size_t count = 0;
        for (const auto& [_, job] : Layout()->Jobs) {
            if (job->WorkerAddress == address) {
                ++count;
            }
        }
        return count;
    };

    auto it = FlowView->State->Workers.begin();
    const std::string hotWorker = it->first;
    const std::string coldWorker = (++it)->first;
    const auto hotIncarnation = FlowView->State->Workers.at(hotWorker)->IncarnationId;

    std::vector<TPartitionId> toMove;
    for (const auto& [_, job] : Layout()->Jobs) {
        if (toMove.size() >= 6) {
            break;
        }
        if (job->WorkerAddress == coldWorker) {
            toMove.push_back(job->PartitionId);
        }
    }
    ASSERT_EQ(toMove.size(), 6u);

    FlowView->State->StartMutation();
    for (const auto& partitionId : toMove) {
        const auto& partition = Layout()->Partitions.at(partitionId);
        Layout()->RemoveJob(partition->CurrentJobId.value(), EJobFinishReason::Rebalanced);
        auto job = New<TJob>();
        job->JobId = TJobId(TGuid::Create());
        job->WorkerAddress = hotWorker;
        job->WorkerIncarnationId = hotIncarnation;
        job->PartitionId = partitionId;
        Layout()->CreateJob(job);
    }
    FlowView->State->CommitMutation();

    const size_t overloadedCount = jobCountOn(hotWorker);
    ASSERT_EQ(overloadedCount, 16u);

    for (int i = 0; i < 10; ++i) {
        Schedule();
    }

    // The spread gate keeps the count-overloaded worker untouched: nothing was kicked off it.
    EXPECT_EQ(jobCountOn(hotWorker), overloadedCount)
        << "overcount kick churned a count-overloaded worker despite the spread gate";
    EXPECT_EQ(PlacedJobCount(), ExecutingPartitionCount());
}

//! 9) The stray exception: even with the spread gate active (huge threshold, even CPU), a worker
//! loss leaves jobless partitions that MUST still be scheduled. HasStrayPartitions() re-opens the
//! fast path, so the orphaned partitions get re-placed on the remaining workers.
TEST_F(TAsyncBalancer, FastRebalanceStrayStillPlacedWhenSpreadSmall)
{
    Prepare(/*computations*/ {"computation"}, /*numWorkers*/ 4);
    DynamicSpec->JobManager->AsyncBalancing = false;
    DynamicSpec->Computations["computation"]->Parameters = ConvertTo<IMapNodePtr>(
        TYsonString(TStringBuf(R""""(
            {
                "desired_partition_count" = 40;
                "partition_count_double_delay" = 3600000000;
                "partition_count_half_delay" = 3600000000;
            }
        )"""")));
    DynamicSpec->JobManager->RebalanceMinCpuSpread = 1000000000.0;
    JobManager->Reconfigure(DynamicSpec);

    for (int i = 0; i < 20 && !(ExecutingPartitionCount() > 0 && PlacedJobCount() >= ExecutingPartitionCount()); ++i) {
        Schedule();
    }
    ASSERT_EQ(PlacedJobCount(), ExecutingPartitionCount());
    ASSERT_GT(PlacedJobCount(), 0u);

    // A worker vanishes: its partitions become stray (jobless).
    {
        FlowView->State->StartMutation();
        auto victim = FlowView->State->Workers.begin()->first;
        FlowView->State->Workers.erase(victim);
        FlowView->State->CommitMutation();
    }

    // RemoveLostJobs (inside Schedule) is what orphans the victim's jobs, so always pump a few rounds
    // rather than gating on a count that only drops after the first Schedule.
    for (int i = 0; i < 20; ++i) {
        Schedule();
    }

    // Despite the spread gate, strays must be scheduled on the remaining workers.
    EXPECT_EQ(PlacedJobCount(), ExecutingPartitionCount())
        << "stray partitions left unplaced under the spread gate";
    for (const auto& [_, job] : Layout()->Jobs) {
        EXPECT_TRUE(FlowView->State->Workers.contains(job->WorkerAddress));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
