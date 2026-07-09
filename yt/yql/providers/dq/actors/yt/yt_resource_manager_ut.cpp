#include "yt_wrapper.h"
#include "resource_manager.h"
#include <yt/yql/providers/dq/global_worker_manager/coordination_helper.h>

#include <contrib/ydb/library/yql/providers/dq/actors/events/events.h>
#include <contrib/ydb/library/yql/providers/dq/common/attrs.h>

#include <contrib/ydb/library/actors/testlib/test_runtime.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <yql/essentials/utils/log/proto/logger_config.pb.h>
#include <yql/essentials/utils/log/log.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/yson/node/node_io.h>

#include <yt/cpp/mapreduce/interface/fluent.h>

using namespace NYql;
using namespace NActors;

// ============================================================================
// Actor-based tests for OnListOperations and deferred release
// ============================================================================

namespace {

// No-op lock actor: TYtResourceManager registers it as a child, but the test
// drives leader transitions manually via TEvBecomeLeader / TEvBecomeFollower.
class TNoopLock : public TActor<TNoopLock> {
public:
    TNoopLock() : TActor(&TNoopLock::StateWork) {}

    STRICT_STFUNC(StateWork, {
        cFunc(TEvents::TEvPoison::EventType, PassAway);
    })
};

// Mock coordinator.
// YtWrapper  → ytActor:    receives YT API calls (ListNode for operations, RemoveNode,
//                          CreateNode, StartOperation, GetOperation, PrintJobStderr)
// CoordWrapper → coordActor: receives worker-node listing (ListWorkers tick),
//                            kept separate so tick noise doesn't pollute ytActor.
class TMockCoordinator : public ICoordinationHelper {
public:
    TMockCoordinator(TActorId ytActor, TActorId coordActor)
        : YtActor_(ytActor)
        , CoordActor_(coordActor)
    {}

    ui32 GetNodeId() override { return 1; }
    ui32 GetNodeId(const TMaybe<ui32>, const TMaybe<TString>&, ui32, ui32,
                   const THashMap<TString, TString>&) override { return 1; }
    TString GetHostname() override { return "localhost"; }
    TString GetIp() override { return "::1"; }

    NActors::IActor* CreateLockOnCluster(NActors::TActorId, const TString&,
                                          const TString&, bool) override {
        return new TNoopLock();
    }
    NActors::IActor* CreateLock(const TString&, bool) override {
        return new TNoopLock();
    }
    NActors::IActor* CreateServiceNodePinger(const IServiceNodeResolver::TPtr&,
                                              const TResourceManagerOptions&,
                                              const THashMap<TString, TString>&) override {
        return nullptr;
    }

    const NProto::TDqConfig::TYtCoordinator& GetConfig() override { return Config_; }
    // CoordinatorWrapper: used by ListWorkers() — tick events go here, not to ytActor
    const NActors::TActorId GetWrapper(NActors::TActorSystem*) override { return CoordActor_; }
    // YtWrapper: used by Bootstrap() and all YT API sends
    const NActors::TActorId GetWrapper(NActors::TActorSystem*, const TString&,
                                        const TString&, const TString&) override { return YtActor_; }
    const NActors::TActorId GetWrapper() override { return CoordActor_; }

    void StartRegistrator(NActors::TActorSystem*) override {}
    void StartGlobalWorker(NActors::TActorSystem*, const TVector<TResourceManagerOptions>&,
                           IMetricsRegistryPtr) override {}
    void StartCleaner(NActors::TActorSystem*, const TMaybe<TString>&) override {}
    NYql::IServiceNodeResolver::TPtr CreateServiceNodeResolver(
        NActors::TActorSystem*, const TVector<TString>&) override { return {}; }
    TWorkerRuntimeData* GetRuntimeData() override { return nullptr; }
    void Stop(NActors::TActorSystem*) override {}
    TString GetRevision() override { return {}; }

private:
    TActorId YtActor_;
    TActorId CoordActor_;
    NProto::TDqConfig::TYtCoordinator Config_;
};

TResourceManagerOptions MakeTestOptions() {
    TResourceManagerOptions opts;
    opts.YtBackend.SetClusterName("localhost-test");
    opts.YtBackend.SetUser("test");
    opts.YtBackend.SetPrefix("//home/test");
    opts.YtBackend.SetMinNodeId(500);
    opts.YtBackend.SetMaxNodeId(1000);
    opts.YtBackend.SetMaxJobs(100);
    opts.YtBackend.SetJobsPerOperation(10);
    opts.UploadPrefix = "//home/test/upload";
    opts.LockName = "ytrm.test";
    opts.TickInterval = TDuration::Max(); // disable ticks in tests
    TResourceFile f;
    f.LocalFileName = "test_worker";
    f.RemoteFileName = "test_worker";
    opts.Files.push_back(f);
    return opts;
}

// Build a Cypress list-node YSON entry for an operation.
// operationId empty → pending (no OPERATIONID_ATTR), non-empty → running.
NYT::TNode MakeOperationEntry(
    const TString& mutationId,
    const TVector<ui32>& nodeIds,
    const TString& operationId = {})
{
    NYT::TNode node(mutationId);
    node.Attributes()["yql_mutation_id"] = mutationId;
    node.Attributes()["yql_command"] = "echo test";
    node.Attributes()["yql_file_paths"] = NYT::TNode::CreateList();

    NYT::TNode idList = NYT::TNode::CreateList();
    for (ui32 id : nodeIds) {
        idList.Add(static_cast<ui64>(id));
    }
    node.Attributes()[NCommonAttrs::ACTOR_NODEID_ATTR] = idList;
    if (!operationId.empty()) {
        node.Attributes()[NCommonAttrs::OPERATIONID_ATTR] = operationId;
    }
    return node;
}

// TEvListNodeResponse carries NYT::TErrorOr<TString> (plain YSON string).
TEvListNodeResponse* MakeListResponse(const TVector<NYT::TNode>& entries) {
    NYT::TNode list = NYT::TNode::CreateList();
    for (const auto& e : entries) {
        list.Add(e);
    }
    NYT::TErrorOr<TString> ok(NYT::NodeToYsonString(list));
    return new TEvListNodeResponse(/*requestId=*/0, ok);
}


void SetupLogging(TTestActorRuntimeBase& /*runtime*/) {
    NYql::NProto::TLoggingConfig loggerConfig;
    loggerConfig.set_allcomponentslevel(NYql::NProto::TLoggingConfig_ELevel_TRACE);
    NYql::NLog::InitLogger(loggerConfig, false);
}


// Drive the resource manager through "BecomeLeader → ListOperations → process list".
// ytActor is the YtWrapper edge actor (receives TEvListNode for operations).
// coordActor absorbs ListWorkers ticks — kept separate so they don't block ytActor grabs.
//
// NO time-based DispatchEvents: those block in TCondVar::WaitD when the queue is empty.
// Instead, GrabEdgeEvent drives dispatch for non-empty lists; for empty lists we use
// FinalEvents=TEvTick (signals the actor entered Leader state after processing the list).
void BecomeLeaderAndProcessList(
    TTestActorRuntimeBase& runtime,
    TActorId rmActor,
    TActorId ytActor,
    const TVector<NYT::TNode>& entries,
    ui32 epoch = 1)
{
    if (epoch == 1) {
        // Wait for TEvBootstrap so YtWrapper is set before TEvBecomeLeader is processed.
        // FinalEvents stops as soon as the event is dispatched — no time-based blocking.
        // Same pattern as global_worker_manager_ut.cpp.
        NActors::TDispatchOptions bootOpts;
        bootOpts.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(bootOpts);
    }

    runtime.Send(new IEventHandle(rmActor, ytActor,
        new TEvBecomeLeader(epoch, "0-0-0-1", "{}")));

    // GrabEdgeEvent dispatches until TEvListNode arrives at ytActor.
    auto listNodeEv = runtime.GrabEdgeEvent<TEvListNode>(ytActor, TDuration::Seconds(5));
    UNIT_ASSERT_C(listNodeEv, "Resource manager did not send TEvListNode");

    runtime.Send(new IEventHandle(rmActor, ytActor, MakeListResponse(entries)));

    // For non-empty lists the caller's GrabEdgeEvent calls drive dispatch naturally.
}

// Trigger operation drop by responding to watcher's TEvGetOperation with ResolveError.
// Returns the RequestId of the subsequent TEvRemoveNode sent by the resource manager.
ui64 TriggerDropAndGetRemoveRequestId(
    TTestActorRuntimeBase& runtime,
    TActorId ytActor)
{
    auto getOpEv = runtime.GrabEdgeEvent<TEvGetOperation>(ytActor, TDuration::Seconds(5));
    UNIT_ASSERT_C(getOpEv, "Expected TEvGetOperation from watcher");

    // ResolveError → watcher sets stopWatcher=true → sends TEvDropOperation to resource manager
    NYT::TErrorOr<TString> resolveErr(
        NYT::TError(NYT::NYTree::EErrorCode::ResolveError, "operation not found"));
    runtime.Send(new IEventHandle(getOpEv->Sender, ytActor,
        new TEvGetOperationResponse(getOpEv->Get()->RequestId, resolveErr)));

    // Drain TEvPrintJobStderr sent by the watcher before it dies
    runtime.GrabEdgeEvent<TEvPrintJobStderr>(ytActor, TDuration::Seconds(5));

    // Resource manager processed TEvDropOperation → sent TEvRemoveNode
    auto removeEv = runtime.GrabEdgeEvent<TEvRemoveNode>(ytActor, TDuration::Seconds(5));
    UNIT_ASSERT_C(removeEv, "Expected TEvRemoveNode after operation drop");
    return removeEv->Get()->RequestId;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(OnListOperationsTest) {

    Y_UNIT_TEST(StaleNodeIsDroppedWhenConflictsWithRunning) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        TActorId ytActor    = runtime.AllocateEdgeActor();
        TActorId coordActor = runtime.AllocateEdgeActor();
        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        TActorId rmActor = runtime.Register(
            CreateResourceManager(MakeTestOptions(), coord));
        runtime.EnableScheduleForActor(rmActor);

        // Pass 1: running OP1 with node_ids=[632,633]
        // Pass 2: stale pending M2 with the same node_ids — must be dropped
        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {
            MakeOperationEntry("c79c6e67-af6adb7a-14765135-8d84dc5", {632, 633},
                               "2460ea77-5ba6a63-3f403e8-55cde5f5"),
            MakeOperationEntry("b1119115-ae3c29c7-72c822ff-e3ae9ef9", {632, 633}),
        });

        // Must send TEvRemoveNode for stale M2 only.
        auto removeEv = runtime.GrabEdgeEvent<TEvRemoveNode>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(removeEv, "Expected TEvRemoveNode for stale operation node");

        const TString& removePath = std::get<0>(*removeEv->Get());
        UNIT_ASSERT_C(removePath.Contains("b1119115"),
            "TEvRemoveNode path should target stale mutation, got: " << removePath);
        UNIT_ASSERT_C(!removePath.Contains("c79c6e67"),
            "Running operation node must NOT be removed");
    }

    Y_UNIT_TEST(PendingWithNoConflictStartsOperation) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        TActorId ytActor    = runtime.AllocateEdgeActor();
        TActorId coordActor = runtime.AllocateEdgeActor();
        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        TActorId rmActor = runtime.Register(
            CreateResourceManager(MakeTestOptions(), coord));
        runtime.EnableScheduleForActor(rmActor);

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {
            MakeOperationEntry("aabbccdd-11223344-aabbccdd-11223344", {700, 701}),
        });

        // StartOrAttachOperation sends TEvCreateNode(-1, CoreTable-NNN) then TEvStartOperation.
        // The coordinator node already exists in Cypress (it's in the list), so no second TEvCreateNode.
        // Drain the CoreTable TEvCreateNode first (requestId=-1).
        auto coreTableEv = runtime.GrabEdgeEvent<TEvCreateNode>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(coreTableEv, "Expected TEvCreateNode (CoreTable) for pending operation");
        UNIT_ASSERT_C(coreTableEv->Get()->RequestId == static_cast<ui64>(-1),
            "First TEvCreateNode should be CoreTable (requestId=-1)");

        // Then the operation start is dispatched.
        auto startEv = runtime.GrabEdgeEvent<TEvStartOperation>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(startEv, "Expected TEvStartOperation for pending operation");
    }
}

Y_UNIT_TEST_SUITE(DeferredNodeIdReleaseTest) {

    // Verifies the core invariant of the fix: node_ids must NOT be released until
    // the coordinator node is confirmed removed from Cypress (TEvRemoveNode response).
    //
    // Strategy: make [632,633] the ONLY available node_ids (MinNodeId=632, MaxNodeId=634).
    // After dropping the running operation, trigger MaybeStartOperations while TEvRemoveNode
    // is still pending (no response sent yet).
    //   - New code: [632,633] are blocked → MaybeStartOperations gets empty nodes → no TEvCreateNode
    //   - Old code: [632,633] are freed immediately → MaybeStartOperations allocates them → TEvCreateNode
    Y_UNIT_TEST(NodeIdsBlockedBeforeRemoveConfirmation) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        TActorId ytActor    = runtime.AllocateEdgeActor();
        TActorId coordActor = runtime.AllocateEdgeActor();

        TResourceManagerOptions opts = MakeTestOptions();
        opts.YtBackend.SetMinNodeId(632);
        opts.YtBackend.SetMaxNodeId(634);  // only IDs 632, 633 available
        opts.YtBackend.SetMaxJobs(2);
        opts.YtBackend.SetJobsPerOperation(2);

        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        TActorId rmActor = runtime.Register(CreateResourceManager(opts, coord));
        runtime.EnableScheduleForActor(rmActor);

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {
            MakeOperationEntry("c79c6e67-af6adb7a-14765135-8d84dc5", {632, 633},
                               "2460ea77-5ba6a63-3f403e8-55cde5f5"),
        });

        // Drop operation — TEvRemoveNode is sent but we do NOT respond to it yet.
        // In new code: [632,633] stay allocated (blocked) until response arrives.
        // In old code: [632,633] are freed immediately.
        TriggerDropAndGetRemoveRequestId(runtime, ytActor);

        // Trigger MaybeStartOperations by simulating a ListWorkers response from coordActor.
        // Leader state handles TEvListNodeResponse via OnListResponse → MaybeStartOperations.
        //
        // runtime.Send is synchronous: OnListResponse runs inside Send, but events SENT BY
        // the handler (TEvCreateNode) are queued in the actor mailbox, not dispatched yet.
        // We need an explicit DispatchEvents to flush that mailbox; if the queue is empty
        // (new code — IDs blocked), TEmptyEventQueueException is thrown.
        bool createNodeSent = false;
        runtime.SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvCreateNode::EventType &&
                ev->GetRecipientRewrite() == ytActor) {
                createNodeSent = true;
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });

        NYT::TErrorOr<TString> emptyList(NYT::NodeToYsonString(NYT::TNode::CreateList()));
        runtime.Send(new IEventHandle(rmActor, coordActor,
            new TEvListNodeResponse(/*requestId=*/0, emptyList)));

        // Flush one event from the mailbox:
        //   old code: TEvCreateNode is queued → dispatched → observer fires → createNodeSent=true
        //   new code: mailbox is empty → TEmptyEventQueueException → createNodeSent stays false
        NActors::TDispatchOptions flushOpts;
        flushOpts.FinalEvents.emplace_back(TEvCreateNode::EventType, 1);
        try {
            runtime.DispatchEvents(flushOpts);
        } catch (const NActors::TEmptyEventQueueException&) {
            // Expected in new code: IDs are blocked, no TEvCreateNode was queued.
        }

        runtime.SetObserverFunc(TTestActorRuntimeBase::DefaultObserverFunc);

        UNIT_ASSERT_C(!createNodeSent,
            "node_ids [632,633] must be blocked while TEvRemoveNode response is pending");
    }

    Y_UNIT_TEST(NodeIdsFreedAfterSuccessfulRemove) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        TActorId ytActor    = runtime.AllocateEdgeActor();
        TActorId coordActor = runtime.AllocateEdgeActor();
        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        TActorId rmActor = runtime.Register(
            CreateResourceManager(MakeTestOptions(), coord));
        runtime.EnableScheduleForActor(rmActor);

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {
            MakeOperationEntry("c79c6e67-af6adb7a-14765135-8d84dc5", {632, 633},
                               "2460ea77-5ba6a63-3f403e8-55cde5f5"),
        });

        ui64 removeRequestId = TriggerDropAndGetRemoveRequestId(runtime, ytActor);

        // Send success response — node_ids 632,633 must be freed.
        NYT::TErrorOr<void> ok;
        runtime.Send(new IEventHandle(rmActor, ytActor, new TEvRemoveNodeResponse(removeRequestId, ok)));
        runtime.Send(new IEventHandle(rmActor, ytActor, new TEvBecomeFollower("{\"yql_actor_node_id\"=0u}")));

        // Epoch 2: re-use the same node_ids [632,633] to prove they were freed.
        // If freeing failed, NodeIdAllocator would log "NODE_ID ALREADY ALLOCATED".
        // GrabEdgeEvent<TEvGetOperation> drives dispatch — no TEvTick needed.
        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {
            MakeOperationEntry("ddccbbaa-44332211-ddccbbaa-44332211", {632, 633},
                               "3571fb88-5ba6a63-3f403e8-aabbccdd"),
        }, /*epoch=*/2);
        auto getOpEv2 = runtime.GrabEdgeEvent<TEvGetOperation>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(getOpEv2, "Epoch 2: node_ids [632,633] should be allocatable after successful remove");
    }

    Y_UNIT_TEST(NodeIdsReleasedOnLeaderEpochChange) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        TActorId ytActor    = runtime.AllocateEdgeActor();
        TActorId coordActor = runtime.AllocateEdgeActor();
        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        TActorId rmActor = runtime.Register(
            CreateResourceManager(MakeTestOptions(), coord));
        runtime.EnableScheduleForActor(rmActor);

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {
            MakeOperationEntry("c79c6e67-af6adb7a-14765135-8d84dc5", {632, 633},
                               "2460ea77-5ba6a63-3f403e8-55cde5f5"),
        });

        ui64 removeRequestId = TriggerDropAndGetRemoveRequestId(runtime, ytActor);

        // Send failure — node_ids 632,633 must stay blocked until epoch change.
        NYT::TErrorOr<void> fail(NYT::TError("Internal RPC call failed"));
        runtime.Send(new IEventHandle(rmActor, ytActor, new TEvRemoveNodeResponse(removeRequestId, fail)));
        // NodeIdAllocator.Clear() on follower transition releases blocked IDs.
        runtime.Send(new IEventHandle(rmActor, ytActor, new TEvBecomeFollower("{\"yql_actor_node_id\"=0u}")));

        // Epoch 2: same node_ids [632,633] must work after Clear() cleared them.
        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {
            MakeOperationEntry("ddccbbaa-44332211-ddccbbaa-44332211", {632, 633},
                               "3571fb88-5ba6a63-3f403e8-aabbccdd"),
        }, /*epoch=*/2);
        auto getOpEv2 = runtime.GrabEdgeEvent<TEvGetOperation>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(getOpEv2, "Epoch 2: node_ids [632,633] should be allocatable after epoch change");
    }
}
