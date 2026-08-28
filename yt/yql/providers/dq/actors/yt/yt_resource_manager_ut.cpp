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

#include <util/stream/file.h>

#include <util/system/fs.h>
#include <util/system/tempfile.h>

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
    NActors::TActorId GetWrapper(NActors::TActorSystem*) override { return CoordActor_; }
    // YtWrapper: used by Bootstrap() and all YT API sends
    NActors::TActorId GetWrapper(NActors::TActorSystem*, const TString&,
                                 const TString&, const TString&) override { return YtActor_; }
    NActors::TActorId GetWrapper() override { return CoordActor_; }

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

bool AllowScheduledEvents(
    TTestActorRuntimeBase& runtime,
    TAutoPtr<IEventHandle>& /*event*/,
    TDuration delay,
    TInstant& deadline)
{
    deadline = runtime.GetTimeProvider()->Now() + delay;
    return false;
}

TResourceManagerOptions MakeTestOptions() {
    TResourceManagerOptions opts;
    opts.YtBackend.SetClusterName("localhost-test");
    opts.YtBackend.SetProxyAddress("localhost-test");
    opts.YtBackend.SetUser("test");
    opts.YtBackend.SetPrefix("//home/test");
    opts.YtBackend.SetMinNodeId(500);
    opts.YtBackend.SetMaxNodeId(1000);
    opts.YtBackend.SetMaxJobs(100);
    opts.YtBackend.SetJobsPerOperation(10);
    opts.UploadPrefix = "//home/test/upload";
    opts.LockName = "ytrm.test";
    opts.TickInterval = TDuration::Max(); // disable ticks in tests
    opts.Counters = opts.Counters
        ->GetSubgroup("component", "dq")
        ->GetSubgroup("counters", "ytrm")
        ->GetSubgroup("ytname", opts.YtBackend.GetClusterName());
    TResourceFile f;
    f.LocalFileName = "test_worker";
    f.RemoteFileName = "test_worker";
    opts.Files.push_back(f);
    return opts;
}

NMonitoring::TDynamicCounters::TCounterPtr GetResourceManagerCounter(
    const TResourceManagerOptions& options,
    const TString& name,
    bool derivative = false)
{
    return options.Counters->GetCounter(name, derivative);
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
    node.Attributes()[NCommonAttrs::OPERATIONSIZE_ATTR] = ssize(nodeIds);
    if (!operationId.empty()) {
        node.Attributes()[NCommonAttrs::OPERATIONID_ATTR] = operationId;
    }
    return node;
}

// TEvListNodeResponse carries NYT::TErrorOr<TString> (plain YSON string).
TEvListNodeResponse* MakeListResponse(const TVector<NYT::TNode>& entries, ui64 requestId = 0) {
    NYT::TNode list = NYT::TNode::CreateList();
    for (const auto& e : entries) {
        list.Add(e);
    }
    NYT::TErrorOr<TString> ok(NYT::NodeToYsonString(list));
    return new TEvListNodeResponse(requestId, ok);
}

NYT::TErrorOr<TString> MakeProvidedSpecResponse(const TVector<ui32>& nodeIds)
{
    NYT::TNode tasks = NYT::TNode::CreateMap();
    for (auto nodeId : nodeIds) {
        NYT::TNode environment = NYT::TNode::CreateMap();
        environment[NCommonJobVars::ACTOR_NODE_ID] = ToString(nodeId);
        environment[NCommonJobVars::OPERATION_SIZE] = ToString(nodeIds.size());

        NYT::TNode task = NYT::TNode::CreateMap();
        task["job_count"] = i64{1};
        task["environment"] = std::move(environment);
        tasks["yql_worker_" + ToString(nodeId)] = std::move(task);
    }

    NYT::TNode providedSpec = NYT::TNode::CreateMap();
    providedSpec["tasks"] = std::move(tasks);
    NYT::TNode operation = NYT::TNode::CreateMap();
    operation["provided_spec"] = std::move(providedSpec);
    return NYT::TErrorOr<TString>(NYT::NodeToYsonString(operation));
}

NYT::TErrorOr<TString> MakeMismatchedProvidedSpecResponse()
{
    auto response = MakeProvidedSpecResponse({701, 702});
    auto operation = NYT::NodeFromYsonString(response.Value());
    auto& environment = operation["provided_spec"]["tasks"]["yql_worker_701"]["environment"];
    environment[NCommonJobVars::ACTOR_NODE_ID] = "702";
    return NYT::TErrorOr<TString>(NYT::NodeToYsonString(operation));
}

void RespondToClaimReconciliation(
    TTestActorRuntimeBase& runtime,
    TActorId rmActor,
    TActorId ytActor,
    const TString& operationId,
    NYT::TErrorOr<TString> response)
{
    auto getOperationEv = runtime.GrabEdgeEvent<TEvGetOperation>(ytActor, TDuration::Seconds(5));
    UNIT_ASSERT_C(getOperationEv, "Expected claim reconciliation request");
    UNIT_ASSERT_VALUES_EQUAL(rmActor, getOperationEv->Sender);
    UNIT_ASSERT_VALUES_EQUAL(operationId, ToString(std::get<0>(*getOperationEv->Get())));
    const auto& options = std::get<1>(*getOperationEv->Get());
    UNIT_ASSERT(options.Attributes);
    UNIT_ASSERT_VALUES_EQUAL(1, options.Attributes->size());
    UNIT_ASSERT(options.Attributes->contains("provided_spec"));
    runtime.Send(new IEventHandle(
        getOperationEv->Sender,
        TActorId(ytActor.NodeId(), "actorsystem"),
        new TEvGetOperationResponse(getOperationEv->Get()->RequestId, std::move(response))));
}

TVector<TVector<ui32>> TriggerCapacityRefresh(
    TTestActorRuntimeBase& runtime,
    TActorId rmActor,
    TActorId ytActor,
    TActorId coordActor)
{
    runtime.Send(new IEventHandle(
        rmActor,
        coordActor,
        new TEvListNodeResponse(
            /*requestId*/ 0,
            NYT::TErrorOr<TString>(NYT::NodeToYsonString(NYT::TNode::CreateList())))));

    TVector<TVector<ui32>> allocatedNodeIds;
    const auto events = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
    for (const auto& event : events) {
        if (event->GetTypeRewrite() != TEvCreateNode::EventType) {
            continue;
        }

        const auto* createNode = event->Get<TEvCreateNode>();
        if (createNode->RequestId != static_cast<ui64>(-1)) {
            allocatedNodeIds.push_back(
                std::get<2>(*createNode).Attributes->Get<TVector<ui32>>(
                    NCommonAttrs::ACTOR_NODEID_ATTR));
        }
    }
    return allocatedNodeIds;
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

    // Async wrapper callbacks use the actor-system service ID as sender.
    runtime.Send(new IEventHandle(
        rmActor,
        TActorId(ytActor.NodeId(), "actorsystem"),
        MakeListResponse(entries, listNodeEv->Get()->RequestId)));

    // For non-empty lists the caller's GrabEdgeEvent calls drive dispatch naturally.
}

// Trigger operation drop by responding to watcher's TEvGetOperation with ResolveError.
// Returns the RequestId of the subsequent TEvRemoveNode sent by the resource manager.
ui64 TriggerDropAndGetRemoveRequestId(
    TTestActorRuntimeBase& runtime,
    TActorId ytActor,
    const TString& operationId = {},
    int watcherCount = 1)
{
    TActorId watcherActor;
    ui64 getOperationRequestId = 0;
    bool watcherFound = false;

    for (int index = 0; index < watcherCount; ++index) {
        auto getOpEv = runtime.GrabEdgeEvent<TEvGetOperation>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(getOpEv, "Expected TEvGetOperation from watcher");

        const auto currentOperationId = ToString(std::get<0>(*getOpEv->Get()));
        if (operationId.empty() || currentOperationId == operationId) {
            UNIT_ASSERT_C(!watcherFound, "More than one watcher found for operation " << operationId);
            watcherActor = getOpEv->Sender;
            getOperationRequestId = getOpEv->Get()->RequestId;
            watcherFound = true;
        }
    }

    UNIT_ASSERT_C(watcherFound, "Watcher not found for operation " << operationId);

    NYT::TErrorOr<TString> resolveErr(
        NYT::TError(NYT::NYTree::EErrorCode::ResolveError, "operation not found"));
    runtime.Send(new IEventHandle(watcherActor, ytActor,
        new TEvGetOperationResponse(getOperationRequestId, resolveErr)));

    runtime.GrabEdgeEvent<TEvPrintJobStderr>(ytActor, TDuration::Seconds(5));

    auto removeEv = runtime.GrabEdgeEvent<TEvRemoveNode>(ytActor, TDuration::Seconds(5));
    UNIT_ASSERT_C(removeEv, "Expected TEvRemoveNode after operation drop");
    return removeEv->Get()->RequestId;
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(OnListOperationsTest) {

    Y_UNIT_TEST(RunningAndPendingOwnersConsumeFullCapacity) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        TActorId ytActor = runtime.AllocateEdgeActor();
        TActorId coordActor = runtime.AllocateEdgeActor();

        TResourceManagerOptions opts = MakeTestOptions();
        opts.YtBackend.SetMinNodeId(700);
        opts.YtBackend.SetMaxNodeId(704);
        opts.YtBackend.SetMaxJobs(4);
        opts.YtBackend.SetJobsPerOperation(2);

        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        TActorId rmActor = runtime.Register(CreateResourceManager(opts, coord));
        runtime.EnableScheduleForActor(rmActor);

        const TString runningMutationId = "c79c6e67-af6adb7a-14765135-8d84dc5";
        const TString runningOperationId = "2460ea77-5ba6a63-3f403e8-55cde5f5";
        const TString pendingMutationId = "b1119115-ae3c29c7-72c822ff-e3ae9ef9";

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {
            MakeOperationEntry(runningMutationId, {700, 701}, runningOperationId),
            MakeOperationEntry(pendingMutationId, {700, 701}),
        });

        NYT::TErrorOr<TString> emptyList(NYT::NodeToYsonString(NYT::TNode::CreateList()));
        runtime.Send(new IEventHandle(
            rmActor,
            coordActor,
            new TEvListNodeResponse(/*requestId*/ 0, emptyList)));

        const auto events = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : events) {
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvStartOperation::EventType);
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvRemoveNode::EventType);
            if (event->GetTypeRewrite() == TEvCreateNode::EventType) {
                UNIT_ASSERT_VALUES_EQUAL(
                    static_cast<ui64>(-1),
                    event->Get<TEvCreateNode>()->RequestId);
            }
        }
    }

    Y_UNIT_TEST(RunningAndPendingConflictKeepsBothClaims) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        TActorId ytActor    = runtime.AllocateEdgeActor();
        TActorId coordActor = runtime.AllocateEdgeActor();

        TResourceManagerOptions opts = MakeTestOptions();
        opts.YtBackend.SetMinNodeId(632);
        opts.YtBackend.SetMaxNodeId(634);
        opts.YtBackend.SetMaxJobs(2);
        opts.YtBackend.SetJobsPerOperation(2);

        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        TActorId rmActor = runtime.Register(CreateResourceManager(opts, coord));
        runtime.EnableScheduleForActor(rmActor);

        const TString runningMutationId = "c79c6e67-af6adb7a-14765135-8d84dc5";
        const TString runningOperationId = "2460ea77-5ba6a63-3f403e8-55cde5f5";
        const TString pendingMutationId = "b1119115-ae3c29c7-72c822ff-e3ae9ef9";

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {
            MakeOperationEntry(runningMutationId, {632, 633}, runningOperationId),
            MakeOperationEntry(pendingMutationId, {632, 633}),
        });

        const auto recoveryConflictingClaimCount = GetResourceManagerCounter(
            opts,
            "recovery_conflicting_claim_count");
        const auto recoveryQuarantinedRecordCount = GetResourceManagerCounter(
            opts,
            "recovery_quarantined_record_count");
        const auto quarantinedOwnerCount = GetResourceManagerCounter(opts, "quarantined_owner_count");
        const auto quarantinedClaimCount = GetResourceManagerCounter(opts, "quarantined_claim_count");
        const auto capacityBlockedByQuarantine = GetResourceManagerCounter(opts, "capacity_blocked_by_quarantine");
        const auto overlappingClaimCount = GetResourceManagerCounter(opts, "overlapping_claim_count");
        UNIT_ASSERT_VALUES_EQUAL(2, recoveryConflictingClaimCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, recoveryQuarantinedRecordCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, quarantinedOwnerCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, quarantinedClaimCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, capacityBlockedByQuarantine->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, overlappingClaimCount->Val());

        auto getOperationEv = runtime.GrabEdgeEvent<TEvGetOperation>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(getOperationEv, "Expected watcher for running operation");
        UNIT_ASSERT_VALUES_EQUAL(runningOperationId, ToString(std::get<0>(*getOperationEv->Get())));

        const auto recoveryEvents = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : recoveryEvents) {
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvRemoveNode::EventType);
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvStartOperation::EventType);
        }

        NYT::TErrorOr<TString> resolveError(
            NYT::TError(NYT::NYTree::EErrorCode::ResolveError, "operation not found"));
        runtime.Send(new IEventHandle(
            getOperationEv->Sender,
            ytActor,
            new TEvGetOperationResponse(getOperationEv->Get()->RequestId, resolveError)));

        runtime.GrabEdgeEvent<TEvPrintJobStderr>(ytActor, TDuration::Seconds(5));

        auto removeEv = runtime.GrabEdgeEvent<TEvRemoveNode>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(removeEv, "Expected durable remove for running operation");

        const TString& removePath = std::get<0>(*removeEv->Get());
        UNIT_ASSERT_C(removePath.Contains(runningMutationId),
            "Remove must target running mutation, got: " << removePath);

        NYT::TErrorOr<void> ok;
        runtime.Send(new IEventHandle(
            rmActor,
            ytActor,
            new TEvRemoveNodeResponse(removeEv->Get()->RequestId, ok)));

        NYT::TErrorOr<TString> emptyList(NYT::NodeToYsonString(NYT::TNode::CreateList()));
        runtime.Send(new IEventHandle(
            rmActor,
            coordActor,
            new TEvListNodeResponse(/*requestId*/ 0, emptyList)));

        const auto allocationEvents = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : allocationEvents) {
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvCreateNode::EventType);
        }
        UNIT_ASSERT_VALUES_EQUAL(1, quarantinedOwnerCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, quarantinedClaimCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, capacityBlockedByQuarantine->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, overlappingClaimCount->Val());

        runtime.Send(new IEventHandle(
            rmActor,
            ytActor,
            new TEvBecomeFollower("{\"yql_actor_node_id\"=0u}")));
        UNIT_ASSERT_VALUES_EQUAL(0, recoveryConflictingClaimCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, recoveryQuarantinedRecordCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, quarantinedOwnerCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, quarantinedClaimCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, capacityBlockedByQuarantine->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, overlappingClaimCount->Val());
    }

    Y_UNIT_TEST(PendingWithoutOperationIdIsQuarantined) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        TActorId ytActor    = runtime.AllocateEdgeActor();
        TActorId coordActor = runtime.AllocateEdgeActor();

        TResourceManagerOptions opts = MakeTestOptions();
        opts.YtBackend.SetMinNodeId(700);
        opts.YtBackend.SetMaxNodeId(704);
        opts.YtBackend.SetMaxJobs(4);
        opts.YtBackend.SetJobsPerOperation(2);

        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        TActorId rmActor = runtime.Register(CreateResourceManager(opts, coord));
        runtime.EnableScheduleForActor(rmActor);

        const TString pendingMutationId = "aabbccdd-11223344-aabbccdd-11223344";

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {
            MakeOperationEntry(pendingMutationId, {700, 701}),
        });

        NYT::TErrorOr<TString> emptyList(NYT::NodeToYsonString(NYT::TNode::CreateList()));
        runtime.Send(new IEventHandle(
            rmActor,
            coordActor,
            new TEvListNodeResponse(/*requestId*/ 0, emptyList)));

        int operationCreateCount = 0;
        const auto events = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : events) {
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvRemoveNode::EventType);
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvStartOperation::EventType);

            if (event->GetTypeRewrite() != TEvCreateNode::EventType) {
                continue;
            }

            const auto* createNode = event->Get<TEvCreateNode>();
            if (createNode->RequestId == static_cast<ui64>(-1)) {
                continue;
            }

            ++operationCreateCount;
            const auto& createPath = std::get<0>(*createNode);
            UNIT_ASSERT_C(!createPath.Contains(pendingMutationId),
                "Fresh allocation must not reuse unresolved mutation: " << createPath);

            const auto& createOptions = std::get<2>(*createNode);
            const auto nodeIds = createOptions.Attributes->Get<TVector<ui32>>(
                NCommonAttrs::ACTOR_NODEID_ATTR);
            UNIT_ASSERT_VALUES_EQUAL(TVector<ui32>({702, 703}), nodeIds);
        }

        UNIT_ASSERT_VALUES_EQUAL(1, operationCreateCount);
        UNIT_ASSERT_VALUES_EQUAL(1, GetResourceManagerCounter(opts, "quarantined_owner_count")->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, GetResourceManagerCounter(opts, "quarantined_claim_count")->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, GetResourceManagerCounter(opts, "capacity_blocked_by_quarantine")->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, GetResourceManagerCounter(opts, "overlapping_claim_count")->Val());
    }

    Y_UNIT_TEST(MissingNodeIdsWithoutOperationIdDoNotBlockCapacity) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        TActorId ytActor = runtime.AllocateEdgeActor();
        TActorId coordActor = runtime.AllocateEdgeActor();

        TResourceManagerOptions opts = MakeTestOptions();
        opts.YtBackend.SetMinNodeId(700);
        opts.YtBackend.SetMaxNodeId(704);
        opts.YtBackend.SetMaxJobs(4);
        opts.YtBackend.SetJobsPerOperation(2);

        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        TActorId rmActor = runtime.Register(CreateResourceManager(opts, coord));
        runtime.EnableScheduleForActor(rmActor);

        const TString malformedMutationId = "aabbccdd-11223344-aabbccdd-11223344";
        NYT::TNode malformedEntry(malformedMutationId);
        malformedEntry.Attributes()["yql_mutation_id"] = malformedMutationId;
        malformedEntry.Attributes()[NCommonAttrs::OPERATIONSIZE_ATTR] = i64{2};

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {malformedEntry});

        const auto recoveryQuarantinedRecordCount = GetResourceManagerCounter(
            opts,
            "recovery_quarantined_record_count");
        const auto incompleteQuarantinedClaimRecordCount = GetResourceManagerCounter(
            opts,
            "incomplete_quarantined_claim_record_count");
        UNIT_ASSERT_VALUES_EQUAL(1, recoveryQuarantinedRecordCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, GetResourceManagerCounter(opts, "quarantined_owner_count")->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, GetResourceManagerCounter(opts, "quarantined_claim_count")->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, GetResourceManagerCounter(opts, "capacity_blocked_by_quarantine")->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, incompleteQuarantinedClaimRecordCount->Val());

        const auto recoveryEvents = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : recoveryEvents) {
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvGetOperation::EventType);
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvStartOperation::EventType);
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvRemoveNode::EventType);
        }

        NYT::TErrorOr<TString> emptyList(NYT::NodeToYsonString(NYT::TNode::CreateList()));
        runtime.Send(new IEventHandle(
            rmActor,
            coordActor,
            new TEvListNodeResponse(/*requestId*/ 0, emptyList)));

        TVector<TVector<ui32>> allocatedNodeIds;
        const auto allocationEvents = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : allocationEvents) {
            if (event->GetTypeRewrite() != TEvCreateNode::EventType) {
                continue;
            }

            const auto* createNode = event->Get<TEvCreateNode>();
            if (createNode->RequestId == static_cast<ui64>(-1)) {
                continue;
            }

            allocatedNodeIds.push_back(
                std::get<2>(*createNode).Attributes->Get<TVector<ui32>>(
                    NCommonAttrs::ACTOR_NODEID_ATTR));
        }
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<TVector<ui32>>({{700, 701}, {702, 703}}),
            allocatedNodeIds);

        runtime.Send(new IEventHandle(
            rmActor,
            ytActor,
            new TEvBecomeFollower("{\"yql_actor_node_id\"=0u}")));
        UNIT_ASSERT_VALUES_EQUAL(0, incompleteQuarantinedClaimRecordCount->Val());
    }

    Y_UNIT_TEST(IncompleteClaimsAreReconciledBeforeAllocation) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        const TActorId ytActor = runtime.AllocateEdgeActor();
        const TActorId coordActor = runtime.AllocateEdgeActor();

        auto opts = MakeTestOptions();
        opts.YtBackend.SetMinNodeId(700);
        opts.YtBackend.SetMaxNodeId(705);
        opts.YtBackend.SetMaxJobs(5);
        opts.YtBackend.SetJobsPerOperation(2);

        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        const TActorId rmActor = runtime.Register(CreateResourceManager(opts, coord));
        runtime.EnableScheduleForActor(rmActor);

        const TString firstOwner = "aabbccdd-11223344-aabbccdd-11223344";
        const TString firstOperationId = "2460ea77-5ba6a63-3f403e8-55cde5f5";
        const TString secondOwner = "b1119115-ae3c29c7-72c822ff-e3ae9ef9";
        const TString secondOperationId = "d4d36d83-1b80d830-bf275284-b57a3051";
        NYT::TNode firstEntry(firstOwner);
        firstEntry.Attributes()["yql_mutation_id"] = firstOwner;
        firstEntry.Attributes()[NCommonAttrs::OPERATIONID_ATTR] = firstOperationId;
        firstEntry.Attributes()[NCommonAttrs::OPERATIONSIZE_ATTR] = i64{1};
        auto secondEntry = MakeOperationEntry(secondOwner, {701}, secondOperationId);
        secondEntry.Attributes()[NCommonAttrs::OPERATIONSIZE_ATTR] = i64{2};
        secondEntry.Attributes()[NCommonAttrs::CLUSTERNAME_ATTR] = NYT::TNode::CreateList();

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {firstEntry, secondEntry});
        auto firstRequest = runtime.GrabEdgeEvent<TEvGetOperation>(ytActor, TDuration::Seconds(5));
        auto secondRequest = runtime.GrabEdgeEvent<TEvGetOperation>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT(firstRequest);
        UNIT_ASSERT(secondRequest);

        const auto respond = [&] (const auto& request) {
            const auto operationId = ToString(std::get<0>(*request->Get()));
            const auto nodeIds = operationId == firstOperationId
                ? TVector<ui32>{700}
                : TVector<ui32>{701, 702};
            runtime.Send(new IEventHandle(
                request->Sender,
                TActorId(ytActor.NodeId(), "actorsystem"),
                new TEvGetOperationResponse(
                    request->Get()->RequestId,
                    MakeProvidedSpecResponse(nodeIds))));
        };

        respond(firstRequest);
        const auto intermediateEvents = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : intermediateEvents) {
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvCreateNode::EventType);
        }

        respond(secondRequest);
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<TVector<ui32>>({{703, 704}}),
            TriggerCapacityRefresh(runtime, rmActor, ytActor, coordActor));
        UNIT_ASSERT_VALUES_EQUAL(1, GetResourceManagerCounter(opts, "recovery_quarantined_record_count")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            GetResourceManagerCounter(opts, "incomplete_quarantined_claim_record_count")->Val());
    }

    Y_UNIT_TEST(ImpossibleClaimsSkipReconciliation) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        const TActorId ytActor = runtime.AllocateEdgeActor();
        const TActorId coordActor = runtime.AllocateEdgeActor();

        auto opts = MakeTestOptions();
        opts.YtBackend.SetMinNodeId(700);
        opts.YtBackend.SetMaxNodeId(705);
        opts.YtBackend.SetMaxJobs(5);
        opts.YtBackend.SetJobsPerOperation(1);

        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        const TActorId rmActor = runtime.Register(CreateResourceManager(opts, coord));
        runtime.EnableScheduleForActor(rmActor);

        const TString firstOwner = "aabbccdd-11223344-aabbccdd-11223344";
        const TString firstOperationId = "2460ea77-5ba6a63-3f403e8-55cde5f5";
        NYT::TNode firstEntry(firstOwner);
        firstEntry.Attributes()["yql_mutation_id"] = firstOwner;
        firstEntry.Attributes()[NCommonAttrs::OPERATIONID_ATTR] = firstOperationId;
        NYT::TNode nodeIds = NYT::TNode::CreateList();
        nodeIds.Add(ui64{700});
        firstEntry.Attributes()[NCommonAttrs::ACTOR_NODEID_ATTR] = std::move(nodeIds);

        const TString secondOwner = "b1119115-ae3c29c7-72c822ff-e3ae9ef9";
        const TString secondOperationId = "d4d36d83-1b80d830-bf275284-b57a3051";
        auto secondEntry = MakeOperationEntry(secondOwner, {701, 702}, secondOperationId);
        secondEntry.Attributes()[NCommonAttrs::OPERATIONSIZE_ATTR] = i64{1};

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {firstEntry, secondEntry});

        const auto recoveryEvents = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : recoveryEvents) {
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvGetOperation::EventType);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            GetResourceManagerCounter(opts, "incomplete_quarantined_claim_record_count")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<TVector<ui32>>({{703}, {704}}),
            TriggerCapacityRefresh(runtime, rmActor, ytActor, coordActor));
    }

    Y_UNIT_TEST(SlowOperationsListResponseCompletesRecovery) {
        TTestActorRuntimeBase runtime;
        runtime.SetScheduledEventFilter(AllowScheduledEvents);
        runtime.Initialize();
        SetupLogging(runtime);

        TActorId ytActor = runtime.AllocateEdgeActor();
        TActorId coordActor = runtime.AllocateEdgeActor();

        TResourceManagerOptions opts = MakeTestOptions();
        opts.TickInterval = TDuration::Seconds(1);
        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        TActorId rmActor = runtime.Register(CreateResourceManager(opts, coord));
        runtime.EnableScheduleForActor(rmActor);

        NActors::TDispatchOptions bootOptions;
        bootOptions.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(bootOptions);

        runtime.Send(new IEventHandle(
            rmActor,
            ytActor,
            new TEvBecomeLeader(/*leaderEpoch*/ 1, "0-0-0-1", "{}")));
        auto listNodeEv = runtime.GrabEdgeEvent<TEvListNode>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(listNodeEv, "Expected operations list request");

        NYT::TNode workerEntry("worker");
        workerEntry.Attributes()[NCommonAttrs::ACTOR_NODEID_ATTR] = ui64{700};
        runtime.Send(new IEventHandle(
            rmActor,
            TActorId(ytActor.NodeId(), "actorsystem"),
            MakeListResponse({workerEntry}, listNodeEv->Get()->RequestId + 1)));

        runtime.SimulateSleep(opts.TickInterval + TDuration::MilliSeconds(1));
        const auto delayedResponseEvents = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        UNIT_ASSERT(delayedResponseEvents.empty());

        const TString mutationId = "c79c6e67-af6adb7a-14765135-8d84dc5";
        const TString operationId = "2460ea77-5ba6a63-3f403e8-55cde5f5";
        runtime.Send(new IEventHandle(
            rmActor,
            TActorId(ytActor.NodeId(), "actorsystem"),
            MakeListResponse(
                {MakeOperationEntry(mutationId, {700, 701}, operationId)},
                listNodeEv->Get()->RequestId)));

        int watcherCount = 0;
        const auto recoveryEvents = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : recoveryEvents) {
            if (event->GetTypeRewrite() != TEvGetOperation::EventType) {
                continue;
            }

            ++watcherCount;
            UNIT_ASSERT_VALUES_EQUAL(operationId, ToString(std::get<0>(*event->Get<TEvGetOperation>())));
        }
        UNIT_ASSERT_VALUES_EQUAL(1, watcherCount);
    }

    Y_UNIT_TEST(MalformedOperationsListResponseIsRetried) {
        TTestActorRuntimeBase runtime;
        runtime.SetScheduledEventFilter(AllowScheduledEvents);
        runtime.Initialize();
        SetupLogging(runtime);

        const TActorId ytActor = runtime.AllocateEdgeActor();
        const TActorId coordActor = runtime.AllocateEdgeActor();

        auto opts = MakeTestOptions();
        opts.TickInterval = TDuration::Seconds(1);
        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        const TActorId rmActor = runtime.Register(CreateResourceManager(opts, coord));
        runtime.EnableScheduleForActor(rmActor);

        NActors::TDispatchOptions bootOptions;
        bootOptions.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(bootOptions);

        runtime.Send(new IEventHandle(
            rmActor,
            ytActor,
            new TEvBecomeLeader(/*leaderEpoch*/ 1, "0-0-0-1", "{}")));
        auto firstListNodeEv = runtime.GrabEdgeEvent<TEvListNode>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(firstListNodeEv, "Expected operations list request");

        NYT::TErrorOr<TString> malformedResponse(TString("{"));
        runtime.Send(new IEventHandle(
            rmActor,
            TActorId(ytActor.NodeId(), "actorsystem"),
            new TEvListNodeResponse(firstListNodeEv->Get()->RequestId, malformedResponse)));

        runtime.SimulateSleep(opts.TickInterval + TDuration::MilliSeconds(1));
        auto retryListNodeEv = runtime.GrabEdgeEvent<TEvListNode>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(retryListNodeEv, "Expected operations list retry");
        UNIT_ASSERT_UNEQUAL(firstListNodeEv->Get()->RequestId, retryListNodeEv->Get()->RequestId);
    }

    Y_UNIT_TEST(LateOperationsListResponseIsIgnoredAfterLeadershipLoss) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        const TActorId ytActor = runtime.AllocateEdgeActor();
        const TActorId coordActor = runtime.AllocateEdgeActor();

        auto opts = MakeTestOptions();
        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        const TActorId rmActor = runtime.Register(CreateResourceManager(opts, coord));
        runtime.EnableScheduleForActor(rmActor);

        NActors::TDispatchOptions bootOptions;
        bootOptions.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        runtime.DispatchEvents(bootOptions);

        runtime.Send(new IEventHandle(
            rmActor,
            ytActor,
            new TEvBecomeLeader(/*leaderEpoch*/ 1, "0-0-0-1", "{}")));
        auto listNodeEv = runtime.GrabEdgeEvent<TEvListNode>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(listNodeEv, "Expected operations list request");

        runtime.Send(new IEventHandle(
            rmActor,
            ytActor,
            new TEvBecomeFollower("{\"yql_actor_node_id\"=0u}")));
        runtime.Send(new IEventHandle(
            rmActor,
            TActorId(ytActor.NodeId(), "actorsystem"),
            MakeListResponse({}, listNodeEv->Get()->RequestId)));

        const auto events = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : events) {
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvCreateNode::EventType);
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvGetOperation::EventType);
        }
    }

    Y_UNIT_TEST(StaleStartResponseDoesNotBlockNextLeaderEpoch) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        TActorId ytActor = runtime.AllocateEdgeActor();
        TActorId coordActor = runtime.AllocateEdgeActor();

        TResourceManagerOptions opts = MakeTestOptions();
        opts.YtBackend.SetMinNodeId(700);
        opts.YtBackend.SetMaxNodeId(704);
        opts.YtBackend.SetMaxJobs(4);
        opts.YtBackend.SetJobsPerOperation(2);

        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        TActorId rmActor = runtime.Register(CreateResourceManager(opts, coord));
        runtime.EnableScheduleForActor(rmActor);

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {});

        NYT::TErrorOr<TString> emptyList(NYT::NodeToYsonString(NYT::TNode::CreateList()));
        runtime.Send(new IEventHandle(
            rmActor,
            coordActor,
            new TEvListNodeResponse(/*requestId*/ 0, emptyList)));

        ui64 createRequestId = 0;
        TString staleMutationId;
        int epochOneOperationCreateCount = 0;
        const auto epochOneEvents = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : epochOneEvents) {
            if (event->GetTypeRewrite() != TEvCreateNode::EventType) {
                continue;
            }

            const auto* createNode = event->Get<TEvCreateNode>();
            if (createNode->RequestId == static_cast<ui64>(-1)) {
                continue;
            }

            ++epochOneOperationCreateCount;
            const auto currentNodeIds = std::get<2>(*createNode).Attributes->Get<TVector<ui32>>(
                NCommonAttrs::ACTOR_NODEID_ATTR);
            if (currentNodeIds != TVector<ui32>({700, 701})) {
                UNIT_ASSERT_VALUES_EQUAL(TVector<ui32>({702, 703}), currentNodeIds);
                continue;
            }

            createRequestId = createNode->RequestId;
            staleMutationId = std::get<2>(*createNode).Attributes->Get<TString>("yql_mutation_id");
        }
        UNIT_ASSERT_VALUES_EQUAL(2, epochOneOperationCreateCount);
        UNIT_ASSERT_UNEQUAL(0, createRequestId);

        NYT::TErrorOr<NYT::NCypressClient::TNodeId> createResult(
            NYT::NCypressClient::TNodeId::FromString(
                "11111111-22222222-33333333-44444444"));
        runtime.Send(new IEventHandle(
            rmActor,
            ytActor,
            new TEvCreateNodeResponse(createRequestId, createResult)));

        auto startOperationEv = runtime.GrabEdgeEvent<TEvStartOperation>(
            ytActor,
            TDuration::Seconds(5));
        UNIT_ASSERT_C(startOperationEv, "Expected start request in the first leader epoch");

        runtime.Send(new IEventHandle(
            rmActor,
            ytActor,
            new TEvBecomeFollower("{\"yql_actor_node_id\"=0u}")));

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {
            MakeOperationEntry(staleMutationId, {700, 701}),
        }, /*epoch*/ 2);

        NYT::TErrorOr<NYT::NScheduler::TOperationId> staleStartResult(
            NYT::NScheduler::TOperationId(
                NYT::TGuid::FromString("55555555-66666666-77777777-88888888")));
        runtime.Send(new IEventHandle(
            rmActor,
            ytActor,
            new TEvStartOperationResponse(
                startOperationEv->Get()->RequestId,
                staleStartResult)));

        runtime.Send(new IEventHandle(
            rmActor,
            coordActor,
            new TEvListNodeResponse(/*requestId*/ 0, emptyList)));

        int operationCreateCount = 0;
        const auto epochTwoEvents = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : epochTwoEvents) {
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvSetNode::EventType);
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvGetOperation::EventType);

            if (event->GetTypeRewrite() != TEvCreateNode::EventType) {
                continue;
            }

            const auto* createNode = event->Get<TEvCreateNode>();
            if (createNode->RequestId == static_cast<ui64>(-1)) {
                continue;
            }

            ++operationCreateCount;
            UNIT_ASSERT_VALUES_EQUAL(
                TVector<ui32>({702, 703}),
                std::get<2>(*createNode).Attributes->Get<TVector<ui32>>(
                    NCommonAttrs::ACTOR_NODEID_ATTR));
        }
        UNIT_ASSERT_VALUES_EQUAL(1, operationCreateCount);
    }

    Y_UNIT_TEST(StaleClaimReconciliationResponseIsIgnored) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        const TActorId ytActor = runtime.AllocateEdgeActor();
        const TActorId coordActor = runtime.AllocateEdgeActor();

        auto opts = MakeTestOptions();
        opts.YtBackend.SetMinNodeId(700);
        opts.YtBackend.SetMaxNodeId(704);
        opts.YtBackend.SetMaxJobs(4);
        opts.YtBackend.SetJobsPerOperation(2);

        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        const TActorId rmActor = runtime.Register(CreateResourceManager(opts, coord));
        runtime.EnableScheduleForActor(rmActor);

        const TString mutationId = "aabbccdd-11223344-aabbccdd-11223344";
        const TString operationId = "2460ea77-5ba6a63-3f403e8-55cde5f5";
        auto incompleteEntry = MakeOperationEntry(mutationId, {}, operationId);
        incompleteEntry.Attributes()[NCommonAttrs::OPERATIONSIZE_ATTR] = i64{2};
        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {incompleteEntry});

        auto staleRequest = runtime.GrabEdgeEvent<TEvGetOperation>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT(staleRequest);

        runtime.Send(new IEventHandle(
            rmActor,
            ytActor,
            new TEvBecomeFollower("{\"yql_actor_node_id\"=0u}")));
        BecomeLeaderAndProcessList(
            runtime,
            rmActor,
            ytActor,
            {MakeOperationEntry(mutationId, {700, 701})},
            /*epoch*/ 2);

        runtime.Send(new IEventHandle(
            rmActor,
            TActorId(ytActor.NodeId(), "actorsystem"),
            new TEvGetOperationResponse(
                staleRequest->Get()->RequestId,
                MakeProvidedSpecResponse({702, 703}))));

        UNIT_ASSERT_VALUES_EQUAL(
            TVector<TVector<ui32>>({{702, 703}}),
            TriggerCapacityRefresh(runtime, rmActor, ytActor, coordActor));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            GetResourceManagerCounter(opts, "incomplete_quarantined_claim_record_count")->Val());
    }

    Y_UNIT_TEST(StaleWatcherCallbackDoesNotDropReAdoptedOperation) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        TActorId ytActor = runtime.AllocateEdgeActor();
        TActorId coordActor = runtime.AllocateEdgeActor();

        TResourceManagerOptions opts = MakeTestOptions();
        opts.YtBackend.SetMinNodeId(700);
        opts.YtBackend.SetMaxNodeId(702);
        opts.YtBackend.SetMaxJobs(2);
        opts.YtBackend.SetJobsPerOperation(2);

        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        TActorId rmActor = runtime.Register(CreateResourceManager(opts, coord));
        runtime.EnableScheduleForActor(rmActor);

        const TString mutationId = "c79c6e67-af6adb7a-14765135-8d84dc5";
        const TString operationId = "2460ea77-5ba6a63-3f403e8-55cde5f5";

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {
            MakeOperationEntry(mutationId, {700, 701}, operationId),
        });

        auto oldGetOperationEv = runtime.GrabEdgeEvent<TEvGetOperation>(
            ytActor,
            TDuration::Seconds(5));
        UNIT_ASSERT_C(oldGetOperationEv, "Expected watcher in the first leader epoch");
        const TActorId oldWatcher = oldGetOperationEv->Sender;

        NYT::TErrorOr<TString> resolveError(
            NYT::TError(NYT::NYTree::EErrorCode::ResolveError, "operation not found"));
        runtime.Send(new IEventHandle(
            oldWatcher,
            ytActor,
            new TEvGetOperationResponse(oldGetOperationEv->Get()->RequestId, resolveError)));
        runtime.GrabEdgeEvent<TEvPrintJobStderr>(ytActor, TDuration::Seconds(5));

        auto staleWatcherEvents = runtime.CaptureMailboxEvents(rmActor.Hint(), rmActor.NodeId());
        UNIT_ASSERT_VALUES_EQUAL(1, staleWatcherEvents.size());
        UNIT_ASSERT_VALUES_EQUAL(oldWatcher, staleWatcherEvents.front()->Sender);

        runtime.Send(new IEventHandle(
            rmActor,
            ytActor,
            new TEvBecomeFollower("{\"yql_actor_node_id\"=0u}")));
        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {
            MakeOperationEntry(mutationId, {700, 701}, operationId),
        }, /*epoch*/ 2);

        auto currentGetOperationEv = runtime.GrabEdgeEvent<TEvGetOperation>(
            ytActor,
            TDuration::Seconds(5));
        UNIT_ASSERT_C(currentGetOperationEv, "Expected watcher in the second leader epoch");
        UNIT_ASSERT_UNEQUAL(oldWatcher, currentGetOperationEv->Sender);

        runtime.PushMailboxEventsFront(rmActor.Hint(), rmActor.NodeId(), staleWatcherEvents);
        NActors::TDispatchOptions dispatchOptions;
        dispatchOptions.FinalEvents.emplace_back([oldWatcher] (IEventHandle& event) {
            return event.Sender == oldWatcher;
        });
        runtime.DispatchEvents(dispatchOptions);

        const auto staleCallbackEvents = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : staleCallbackEvents) {
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvRemoveNode::EventType);
        }

        runtime.Send(new IEventHandle(
            currentGetOperationEv->Sender,
            ytActor,
            new TEvGetOperationResponse(currentGetOperationEv->Get()->RequestId, resolveError)));
        runtime.GrabEdgeEvent<TEvPrintJobStderr>(ytActor, TDuration::Seconds(5));
        auto removeEv = runtime.GrabEdgeEvent<TEvRemoveNode>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(removeEv, "Expected current watcher callback to remove the operation");
    }

    Y_UNIT_TEST(OperationIdWriteRetriesWithinLeaderEpoch) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        TActorId ytActor = runtime.AllocateEdgeActor();
        TActorId coordActor = runtime.AllocateEdgeActor();

        TResourceManagerOptions opts = MakeTestOptions();
        opts.YtBackend.SetMinNodeId(700);
        opts.YtBackend.SetMaxNodeId(702);
        opts.YtBackend.SetMaxJobs(2);
        opts.YtBackend.SetJobsPerOperation(2);

        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        TActorId rmActor = runtime.Register(CreateResourceManager(opts, coord));
        runtime.EnableScheduleForActor(rmActor);

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {});

        NYT::TErrorOr<TString> emptyList(NYT::NodeToYsonString(NYT::TNode::CreateList()));
        runtime.Send(new IEventHandle(
            rmActor,
            coordActor,
            new TEvListNodeResponse(/*requestId*/ 0, emptyList)));

        ui64 createRequestId = 0;
        const auto createEvents = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : createEvents) {
            if (event->GetTypeRewrite() != TEvCreateNode::EventType) {
                continue;
            }

            const auto* createNode = event->Get<TEvCreateNode>();
            if (createNode->RequestId != static_cast<ui64>(-1)) {
                createRequestId = createNode->RequestId;
            }
        }
        UNIT_ASSERT_UNEQUAL(0, createRequestId);

        NYT::TErrorOr<NYT::NCypressClient::TNodeId> createResult(
            NYT::NCypressClient::TNodeId::FromString(
                "11111111-22222222-33333333-44444444"));
        runtime.Send(new IEventHandle(
            rmActor,
            ytActor,
            new TEvCreateNodeResponse(createRequestId, createResult)));

        auto startOperationEv = runtime.GrabEdgeEvent<TEvStartOperation>(
            ytActor,
            TDuration::Seconds(5));
        UNIT_ASSERT_C(startOperationEv, "Expected start operation request");

        const TString operationId = "2460ea77-5ba6a63-3f403e8-55cde5f5";
        NYT::TErrorOr<NYT::NScheduler::TOperationId> startResult(
            NYT::NScheduler::TOperationId(NYT::TGuid::FromString(operationId)));
        runtime.Send(new IEventHandle(
            rmActor,
            ytActor,
            new TEvStartOperationResponse(startOperationEv->Get()->RequestId, startResult)));

        auto firstSetNodeEv = runtime.GrabEdgeEvent<TEvSetNode>(
            ytActor,
            TDuration::Seconds(5));
        UNIT_ASSERT_C(firstSetNodeEv, "Expected operation ID write");
        const auto pendingOperationIdUpdateCount = GetResourceManagerCounter(
            opts,
            "pending_operation_id_update_count");
        UNIT_ASSERT_VALUES_EQUAL(1, pendingOperationIdUpdateCount->Val());

        NYT::TErrorOr<void> setError(NYT::TError("Set operation ID failed"));
        runtime.Send(new IEventHandle(
            rmActor,
            ytActor,
            new TEvSetNodeResponse(firstSetNodeEv->Get()->RequestId, setError)));

        const auto preTickEvents = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : preTickEvents) {
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvSetNode::EventType);
        }
        UNIT_ASSERT_VALUES_EQUAL(1, pendingOperationIdUpdateCount->Val());

        runtime.Send(new IEventHandle(rmActor, ytActor, new TEvTick()));

        TEvSetNode* retrySetNode = nullptr;
        const auto retryEvents = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : retryEvents) {
            if (event->GetTypeRewrite() == TEvSetNode::EventType) {
                UNIT_ASSERT_C(!retrySetNode, "Expected exactly one operation ID retry");
                retrySetNode = event->Get<TEvSetNode>();
            }
        }
        UNIT_ASSERT_C(retrySetNode, "Expected operation ID write retry on tick");
        UNIT_ASSERT_VALUES_EQUAL(std::get<0>(*firstSetNodeEv->Get()), std::get<0>(*retrySetNode));
        UNIT_ASSERT_VALUES_EQUAL(std::get<1>(*firstSetNodeEv->Get()).ToString(), std::get<1>(*retrySetNode).ToString());
        UNIT_ASSERT(
            std::get<2>(*firstSetNodeEv->Get()).PrerequisiteTransactionIds ==
            std::get<2>(*retrySetNode).PrerequisiteTransactionIds);

        NYT::TErrorOr<void> setOk;
        runtime.Send(new IEventHandle(
            rmActor,
            ytActor,
            new TEvSetNodeResponse(retrySetNode->RequestId, setOk)));

        runtime.Send(new IEventHandle(rmActor, ytActor, new TEvTick()));

        const auto postSuccessEvents = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : postSuccessEvents) {
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvSetNode::EventType);
        }
        UNIT_ASSERT_VALUES_EQUAL(0, pendingOperationIdUpdateCount->Val());
    }

    Y_UNIT_TEST(PartialInvalidClaimsRemainQuarantinedWhenReconciliationFails) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        TActorId ytActor = runtime.AllocateEdgeActor();
        TActorId coordActor = runtime.AllocateEdgeActor();

        TResourceManagerOptions opts = MakeTestOptions();
        opts.YtBackend.SetMinNodeId(700);
        opts.YtBackend.SetMaxNodeId(706);
        opts.YtBackend.SetMaxJobs(6);
        opts.YtBackend.SetJobsPerOperation(1);

        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        TActorId rmActor = runtime.Register(CreateResourceManager(opts, coord));
        runtime.EnableScheduleForActor(rmActor);

        const TString firstOperationId = "2460ea77-5ba6a63-3f403e8-55cde5f5";
        auto firstEntry = MakeOperationEntry(
            "c79c6e67-af6adb7a-14765135-8d84dc5",
            {},
            firstOperationId);
        NYT::TNode firstNodeIds = NYT::TNode::CreateList();
        firstNodeIds.Add(ui64{700});
        firstNodeIds.Add("invalid");
        firstEntry.Attributes()[NCommonAttrs::ACTOR_NODEID_ATTR] = firstNodeIds;
        firstEntry.Attributes()[NCommonAttrs::OPERATIONSIZE_ATTR] = i64{2};

        const TString secondOperationId = "d4d36d83-1b80d830-bf275284-b57a3051";
        auto secondEntry = MakeOperationEntry(
            "b1119115-ae3c29c7-72c822ff-e3ae9ef9",
            {},
            secondOperationId);
        NYT::TNode secondNodeIds = NYT::TNode::CreateList();
        secondNodeIds.Add(ui64{701});
        secondNodeIds.Add("invalid");
        secondEntry.Attributes()[NCommonAttrs::ACTOR_NODEID_ATTR] = secondNodeIds;
        secondEntry.Attributes()[NCommonAttrs::OPERATIONSIZE_ATTR] = i64{2};

        const TString thirdOperationId = "f3de794a-55499ce8-19d354bb-ef9ef6b8";
        auto thirdEntry = MakeOperationEntry(
            "78690811-f4d562a6-72c822ff-e3ae9ef9",
            {},
            thirdOperationId);
        NYT::TNode thirdNodeIds = NYT::TNode::CreateList();
        thirdNodeIds.Add(ui64{702});
        thirdNodeIds.Add("invalid");
        thirdEntry.Attributes()[NCommonAttrs::ACTOR_NODEID_ATTR] = thirdNodeIds;
        thirdEntry.Attributes()[NCommonAttrs::OPERATIONSIZE_ATTR] = i64{2};

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {firstEntry, secondEntry, thirdEntry});
        RespondToClaimReconciliation(
            runtime,
            rmActor,
            ytActor,
            firstOperationId,
            NYT::TErrorOr<TString>(NYT::TError("Get operation failed")));
        RespondToClaimReconciliation(
            runtime,
            rmActor,
            ytActor,
            secondOperationId,
            MakeMismatchedProvidedSpecResponse());
        RespondToClaimReconciliation(
            runtime,
            rmActor,
            ytActor,
            thirdOperationId,
            MakeProvidedSpecResponse({702}));

        const auto recoveryQuarantinedRecordCount = GetResourceManagerCounter(
            opts,
            "recovery_quarantined_record_count");
        UNIT_ASSERT_VALUES_EQUAL(3, recoveryQuarantinedRecordCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(3, GetResourceManagerCounter(opts, "quarantined_owner_count")->Val());
        UNIT_ASSERT_VALUES_EQUAL(3, GetResourceManagerCounter(opts, "quarantined_claim_count")->Val());
        UNIT_ASSERT_VALUES_EQUAL(3, GetResourceManagerCounter(opts, "capacity_blocked_by_quarantine")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            GetResourceManagerCounter(opts, "incomplete_quarantined_claim_record_count")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            TVector<TVector<ui32>>({{703}, {704}, {705}}),
            TriggerCapacityRefresh(runtime, rmActor, ytActor, coordActor));
    }

    Y_UNIT_TEST(RecoverableInvalidMetadataQuarantinesOnlyItsOwner) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        TActorId ytActor = runtime.AllocateEdgeActor();
        TActorId coordActor = runtime.AllocateEdgeActor();

        TResourceManagerOptions opts = MakeTestOptions();
        opts.YtBackend.SetMinNodeId(700);
        opts.YtBackend.SetMaxNodeId(704);
        opts.YtBackend.SetMaxJobs(4);
        opts.YtBackend.SetJobsPerOperation(2);

        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        TActorId rmActor = runtime.Register(CreateResourceManager(opts, coord));
        runtime.EnableScheduleForActor(rmActor);

        const TString canonicalOwner = "c79c6e67-af6adb7a-14765135-8d84dc5";
        const TString operationId = "2460ea77-5ba6a63-3f403e8-55cde5f5";
        auto invalidEntry = MakeOperationEntry(canonicalOwner, {700, 701}, operationId);
        invalidEntry.Attributes()["yql_mutation_id"] =
            "b1119115-ae3c29c7-72c822ff-e3ae9ef9";

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {invalidEntry});

        NYT::TErrorOr<TString> emptyList(NYT::NodeToYsonString(NYT::TNode::CreateList()));
        runtime.Send(new IEventHandle(
            rmActor,
            coordActor,
            new TEvListNodeResponse(/*requestId*/ 0, emptyList)));

        int operationCreateCount = 0;
        const auto events = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : events) {
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvGetOperation::EventType);
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvStartOperation::EventType);
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvRemoveNode::EventType);

            if (event->GetTypeRewrite() != TEvCreateNode::EventType) {
                continue;
            }

            const auto* createNode = event->Get<TEvCreateNode>();
            if (createNode->RequestId == static_cast<ui64>(-1)) {
                continue;
            }

            ++operationCreateCount;
            UNIT_ASSERT_VALUES_EQUAL(
                TVector<ui32>({702, 703}),
                std::get<2>(*createNode).Attributes->Get<TVector<ui32>>(
                    NCommonAttrs::ACTOR_NODEID_ATTR));
        }
        UNIT_ASSERT_VALUES_EQUAL(1, operationCreateCount);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            GetResourceManagerCounter(opts, "incomplete_quarantined_claim_record_count")->Val());
    }

    Y_UNIT_TEST(OperationSizeMismatchMakesClaimIncomplete) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        const TActorId ytActor = runtime.AllocateEdgeActor();
        const TActorId coordActor = runtime.AllocateEdgeActor();

        auto opts = MakeTestOptions();
        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        const TActorId rmActor = runtime.Register(CreateResourceManager(opts, coord));
        runtime.EnableScheduleForActor(rmActor);

        auto entry = MakeOperationEntry(
            "c79c6e67-af6adb7a-14765135-8d84dc5",
            {700});
        entry.Attributes()[NCommonAttrs::OPERATIONSIZE_ATTR] = i64{2};

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {entry});

        UNIT_ASSERT_VALUES_EQUAL(1, GetResourceManagerCounter(opts, "quarantined_owner_count")->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, GetResourceManagerCounter(opts, "quarantined_claim_count")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            GetResourceManagerCounter(opts, "incomplete_quarantined_claim_record_count")->Val());
    }

    Y_UNIT_TEST(StartFailureKeepsClaimQuarantined) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        TActorId ytActor = runtime.AllocateEdgeActor();
        TActorId coordActor = runtime.AllocateEdgeActor();

        TResourceManagerOptions opts = MakeTestOptions();
        opts.YtBackend.SetMinNodeId(700);
        opts.YtBackend.SetMaxNodeId(702);
        opts.YtBackend.SetMaxJobs(2);
        opts.YtBackend.SetJobsPerOperation(2);

        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        TActorId rmActor = runtime.Register(CreateResourceManager(opts, coord));
        runtime.EnableScheduleForActor(rmActor);

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {});

        NYT::TErrorOr<TString> emptyList(NYT::NodeToYsonString(NYT::TNode::CreateList()));
        runtime.Send(new IEventHandle(
            rmActor,
            coordActor,
            new TEvListNodeResponse(/*requestId*/ 0, emptyList)));

        ui64 createRequestId = 0;
        const auto createEvents = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : createEvents) {
            if (event->GetTypeRewrite() != TEvCreateNode::EventType) {
                continue;
            }
            const auto* createNode = event->Get<TEvCreateNode>();
            if (createNode->RequestId != static_cast<ui64>(-1)) {
                createRequestId = createNode->RequestId;
            }
        }
        UNIT_ASSERT_UNEQUAL(0, createRequestId);

        NYT::TErrorOr<NYT::NCypressClient::TNodeId> createResult(
            NYT::NCypressClient::TNodeId::FromString(
                "11111111-22222222-33333333-44444444"));
        runtime.Send(new IEventHandle(
            rmActor,
            ytActor,
            new TEvCreateNodeResponse(createRequestId, createResult)));
        auto startOperationEv = runtime.GrabEdgeEvent<TEvStartOperation>(
            ytActor,
            TDuration::Seconds(5));
        UNIT_ASSERT_C(startOperationEv, "Expected start operation request");

        NYT::TErrorOr<NYT::NScheduler::TOperationId> startError(
            NYT::TError("Start operation outcome is unknown"));
        runtime.Send(new IEventHandle(
            rmActor,
            ytActor,
            new TEvStartOperationResponse(startOperationEv->Get()->RequestId, startError)));
        runtime.Send(new IEventHandle(
            rmActor,
            coordActor,
            new TEvListNodeResponse(/*requestId*/ 0, emptyList)));

        const auto retryEvents = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : retryEvents) {
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvStartOperation::EventType);
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvRemoveNode::EventType);
            if (event->GetTypeRewrite() == TEvCreateNode::EventType) {
                UNIT_ASSERT_VALUES_EQUAL(
                    static_cast<ui64>(-1),
                    event->Get<TEvCreateNode>()->RequestId);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(1, GetResourceManagerCounter(opts, "quarantined_owner_count")->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, GetResourceManagerCounter(opts, "quarantined_claim_count")->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, GetResourceManagerCounter(opts, "capacity_blocked_by_quarantine")->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            GetResourceManagerCounter(opts, "unknown_start_outcome_count", /*derivative*/ true)->Val());
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            GetResourceManagerCounter(opts, "unknown_start_outcome_claim_count", /*derivative*/ true)->Val());
    }

    Y_UNIT_TEST(OperationPreparationFailureReleasesNodeIds) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        TActorId ytActor = runtime.AllocateEdgeActor();
        TActorId coordActor = runtime.AllocateEdgeActor();

        const auto tokenPath = MakeTempName();
        TTempFile tokenFile(tokenPath);
        NFs::Remove(tokenPath);

        TResourceManagerOptions opts = MakeTestOptions();
        opts.YtBackend.SetMinNodeId(700);
        opts.YtBackend.SetMaxNodeId(702);
        opts.YtBackend.SetMaxJobs(2);
        opts.YtBackend.SetJobsPerOperation(2);
        auto* vaultEnv = opts.YtBackend.AddVaultEnv();
        vaultEnv->SetName("TOKEN");
        vaultEnv->SetValue(tokenPath);

        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        TActorId rmActor = runtime.Register(CreateResourceManager(opts, coord));
        runtime.EnableScheduleForActor(rmActor);

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {});

        NYT::TErrorOr<TString> emptyList(NYT::NodeToYsonString(NYT::TNode::CreateList()));
        runtime.Send(new IEventHandle(
            rmActor,
            coordActor,
            new TEvListNodeResponse(/*requestId*/ 0, emptyList)));

        const auto failedAttemptEvents = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : failedAttemptEvents) {
            UNIT_ASSERT_UNEQUAL(event->GetTypeRewrite(), TEvCreateNode::EventType);
        }

        {
            TFileOutput tokenOutput(tokenPath);
            tokenOutput << "token\n";
        }

        runtime.Send(new IEventHandle(
            rmActor,
            coordActor,
            new TEvListNodeResponse(/*requestId*/ 0, emptyList)));

        int operationCreateCount = 0;
        const auto retryEvents = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : retryEvents) {
            if (event->GetTypeRewrite() != TEvCreateNode::EventType) {
                continue;
            }

            const auto* createNode = event->Get<TEvCreateNode>();
            if (createNode->RequestId == static_cast<ui64>(-1)) {
                continue;
            }

            ++operationCreateCount;
            UNIT_ASSERT_VALUES_EQUAL(
                TVector<ui32>({700, 701}),
                std::get<2>(*createNode).Attributes->Get<TVector<ui32>>(
                    NCommonAttrs::ACTOR_NODEID_ATTR));
        }
        UNIT_ASSERT_VALUES_EQUAL(1, operationCreateCount);
    }

    Y_UNIT_TEST(CreateNodeFailureUsesForcedCleanup) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        TActorId ytActor = runtime.AllocateEdgeActor();
        TActorId coordActor = runtime.AllocateEdgeActor();

        TResourceManagerOptions opts = MakeTestOptions();
        opts.YtBackend.SetMinNodeId(700);
        opts.YtBackend.SetMaxNodeId(702);
        opts.YtBackend.SetMaxJobs(2);
        opts.YtBackend.SetJobsPerOperation(2);

        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        TActorId rmActor = runtime.Register(CreateResourceManager(opts, coord));
        runtime.EnableScheduleForActor(rmActor);

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {});

        NYT::TErrorOr<TString> emptyList(NYT::NodeToYsonString(NYT::TNode::CreateList()));
        runtime.Send(new IEventHandle(
            rmActor,
            coordActor,
            new TEvListNodeResponse(/*requestId*/ 0, emptyList)));

        ui64 createRequestId = 0;
        const auto createEvents = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : createEvents) {
            if (event->GetTypeRewrite() != TEvCreateNode::EventType) {
                continue;
            }

            const auto* createNode = event->Get<TEvCreateNode>();
            if (createNode->RequestId != static_cast<ui64>(-1)) {
                createRequestId = createNode->RequestId;
            }
        }
        UNIT_ASSERT_UNEQUAL(0, createRequestId);

        NYT::TErrorOr<NYT::NCypressClient::TNodeId> createError(
            NYT::TError("Create operation node failed"));
        runtime.Send(new IEventHandle(
            rmActor,
            ytActor,
            new TEvCreateNodeResponse(createRequestId, createError)));

        auto removeEv = runtime.GrabEdgeEvent<TEvRemoveNode>(ytActor, TDuration::Seconds(5));
        UNIT_ASSERT_C(removeEv, "Expected operation node cleanup");
        UNIT_ASSERT(std::get<1>(*removeEv->Get()).Force);
    }
}

Y_UNIT_TEST_SUITE(DeferredNodeIdReleaseTest) {

    Y_UNIT_TEST(SuccessfulRemoveDoesNotFreeOtherOwnerNodeIds) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        TActorId ytActor = runtime.AllocateEdgeActor();
        TActorId coordActor = runtime.AllocateEdgeActor();

        TResourceManagerOptions opts = MakeTestOptions();
        opts.YtBackend.SetMinNodeId(632);
        opts.YtBackend.SetMaxNodeId(634);
        opts.YtBackend.SetMaxJobs(2);
        opts.YtBackend.SetJobsPerOperation(2);

        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        TActorId rmActor = runtime.Register(CreateResourceManager(opts, coord));
        runtime.EnableScheduleForActor(rmActor);

        const TString firstMutationId = "c79c6e67-af6adb7a-14765135-8d84dc5";
        const TString firstOperationId = "2460ea77-5ba6a63-3f403e8-55cde5f5";
        const TString secondMutationId = "b1119115-ae3c29c7-72c822ff-e3ae9ef9";
        const TString secondOperationId = "3571fb88-5ba6a63-3f403e8-aabbccdd";

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {
            MakeOperationEntry(firstMutationId, {632, 633}, firstOperationId),
            MakeOperationEntry(secondMutationId, {632, 633}, secondOperationId),
        });

        const ui64 removeRequestId = TriggerDropAndGetRemoveRequestId(
            runtime,
            ytActor,
            firstOperationId,
            /*watcherCount*/ 2);

        NYT::TErrorOr<void> ok;
        runtime.Send(new IEventHandle(
            rmActor,
            ytActor,
            new TEvRemoveNodeResponse(removeRequestId, ok)));

        NYT::TErrorOr<TString> emptyList(NYT::NodeToYsonString(NYT::TNode::CreateList()));
        runtime.Send(new IEventHandle(
            rmActor,
            coordActor,
            new TEvListNodeResponse(/*requestId*/ 0, emptyList)));

        bool createNodeSent = false;
        const auto events = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : events) {
            if (event->GetTypeRewrite() == TEvCreateNode::EventType) {
                createNodeSent = true;
            }
        }

        UNIT_ASSERT_C(
            !createNodeSent,
            "Removing one owner must not make node IDs available while another owner claims them");
    }

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

        TActorId ytActor = runtime.AllocateEdgeActor();
        TActorId coordActor = runtime.AllocateEdgeActor();

        TResourceManagerOptions opts = MakeTestOptions();
        opts.YtBackend.SetMinNodeId(632);
        opts.YtBackend.SetMaxNodeId(634);
        opts.YtBackend.SetMaxJobs(2);
        opts.YtBackend.SetJobsPerOperation(2);

        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        TActorId rmActor = runtime.Register(CreateResourceManager(opts, coord));
        runtime.EnableScheduleForActor(rmActor);

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {
            MakeOperationEntry("c79c6e67-af6adb7a-14765135-8d84dc5", {632, 633},
                               "2460ea77-5ba6a63-3f403e8-55cde5f5"),
        });

        ui64 removeRequestId = TriggerDropAndGetRemoveRequestId(runtime, ytActor);

        NYT::TErrorOr<void> ok;
        runtime.Send(new IEventHandle(rmActor, ytActor, new TEvRemoveNodeResponse(removeRequestId, ok)));

        NYT::TErrorOr<TString> emptyList(NYT::NodeToYsonString(NYT::TNode::CreateList()));
        runtime.Send(new IEventHandle(
            rmActor,
            coordActor,
            new TEvListNodeResponse(/*requestId*/ 0, emptyList)));

        int operationCreateCount = 0;
        const auto events = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : events) {
            if (event->GetTypeRewrite() != TEvCreateNode::EventType) {
                continue;
            }

            const auto* createNode = event->Get<TEvCreateNode>();
            if (createNode->RequestId == static_cast<ui64>(-1)) {
                continue;
            }

            ++operationCreateCount;
            UNIT_ASSERT_VALUES_EQUAL(
                TVector<ui32>({632, 633}),
                std::get<2>(*createNode).Attributes->Get<TVector<ui32>>(
                    NCommonAttrs::ACTOR_NODEID_ATTR));
        }
        UNIT_ASSERT_VALUES_EQUAL(1, operationCreateCount);
    }

    Y_UNIT_TEST(FailedRemoveReleasesNodeIdsOnlyOnNextEpoch) {
        TTestActorRuntimeBase runtime;
        runtime.Initialize();
        SetupLogging(runtime);

        TActorId ytActor = runtime.AllocateEdgeActor();
        TActorId coordActor = runtime.AllocateEdgeActor();

        TResourceManagerOptions opts = MakeTestOptions();
        opts.YtBackend.SetMinNodeId(632);
        opts.YtBackend.SetMaxNodeId(634);
        opts.YtBackend.SetMaxJobs(2);
        opts.YtBackend.SetJobsPerOperation(2);

        auto coord = MakeIntrusive<TMockCoordinator>(ytActor, coordActor);
        TActorId rmActor = runtime.Register(CreateResourceManager(opts, coord));
        runtime.EnableScheduleForActor(rmActor);

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {
            MakeOperationEntry("c79c6e67-af6adb7a-14765135-8d84dc5", {632, 633},
                               "2460ea77-5ba6a63-3f403e8-55cde5f5"),
        });

        ui64 removeRequestId = TriggerDropAndGetRemoveRequestId(runtime, ytActor);

        NYT::TErrorOr<void> fail(NYT::TError("Internal RPC call failed"));
        runtime.Send(new IEventHandle(rmActor, ytActor, new TEvRemoveNodeResponse(removeRequestId, fail)));

        NYT::TErrorOr<TString> emptyList(NYT::NodeToYsonString(NYT::TNode::CreateList()));
        runtime.Send(new IEventHandle(
            rmActor,
            coordActor,
            new TEvListNodeResponse(/*requestId*/ 0, emptyList)));

        const auto sameEpochEvents = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : sameEpochEvents) {
            if (event->GetTypeRewrite() == TEvCreateNode::EventType) {
                UNIT_ASSERT_VALUES_EQUAL(
                    static_cast<ui64>(-1),
                    event->Get<TEvCreateNode>()->RequestId);
            }
        }

        runtime.Send(new IEventHandle(rmActor, ytActor, new TEvBecomeFollower("{\"yql_actor_node_id\"=0u}")));

        BecomeLeaderAndProcessList(runtime, rmActor, ytActor, {}, /*epoch*/ 2);
        runtime.Send(new IEventHandle(
            rmActor,
            coordActor,
            new TEvListNodeResponse(/*requestId*/ 0, emptyList)));

        int operationCreateCount = 0;
        const auto nextEpochEvents = runtime.CaptureMailboxEvents(ytActor.Hint(), ytActor.NodeId());
        for (const auto& event : nextEpochEvents) {
            if (event->GetTypeRewrite() != TEvCreateNode::EventType) {
                continue;
            }

            const auto* createNode = event->Get<TEvCreateNode>();
            if (createNode->RequestId == static_cast<ui64>(-1)) {
                continue;
            }

            ++operationCreateCount;
            UNIT_ASSERT_VALUES_EQUAL(
                TVector<ui32>({632, 633}),
                std::get<2>(*createNode).Attributes->Get<TVector<ui32>>(
                    NCommonAttrs::ACTOR_NODEID_ATTR));
        }
        UNIT_ASSERT_VALUES_EQUAL(1, operationCreateCount);
    }
}
