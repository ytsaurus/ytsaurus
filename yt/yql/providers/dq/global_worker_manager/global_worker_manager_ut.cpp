#include "global_worker_manager.h"
#include <yt/yql/providers/dq/testlib/test_coordination_helper.h>
#include <yt/yql/providers/dq/actors/yt/yt_wrapper.h>
#include <contrib/ydb/library/yql/providers/dq/worker_manager/local_worker_manager.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/interconnect/interconnect.h>
#include <contrib/ydb/library/yql/providers/dq/worker_manager/interface/events.h>

#include <yql/essentials/utils/log/proto/logger_config.pb.h>
#include <yql/essentials/utils/log/log.h>

#include <library/cpp/testing/unittest/registar.h>
#include <contrib/ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/yson/node/node_io.h>
#include <yt/cpp/mapreduce/interface/fluent.h>
#include <yql/essentials/providers/common/metrics/metrics_registry.h>
#include <contrib/ydb/library/yql/providers/dq/actors/events/events.h>
#include <contrib/ydb/library/yql/providers/dq/common/attrs.h>
#include <contrib/ydb/library/yql/providers/dq/common/yql_dq_common.h>
#include <yt/yql/providers/dq/actors/dynamic_nameserver.h>
#include <contrib/ydb/library/yql/providers/dq/actors/resource_allocator.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/core/dq_integration/transform/yql_dq_task_transform.h>
#include <contrib/ydb/library/yql/dq/comp_nodes/yql_common_dq_factory.h>
#include <yql/essentials/providers/common/comp_nodes/yql_factory.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <contrib/ydb/library/yql/dq/transform/yql_common_dq_transform.h>
#include <contrib/ydb/library/yql/providers/dq/task_runner/tasks_runner_local.h>

#include <util/generic/map.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>

using namespace NYql;
using namespace NActors;
using namespace NProto;
using namespace NDqs;

namespace {

using TWarmupGroups = TMap<std::pair<TString, TString>, THashSet<TString>>;

template <typename TGroups>
void AssertWarmupGroups(const TGroups& groups, const TWarmupGroups& expected) {
    UNIT_ASSERT_VALUES_EQUAL(groups.size(), expected.size());
    THashSet<std::pair<TString, TString>> locations;
    for (const auto& group : groups) {
        const auto location = std::make_pair(group.GetUploadClusterAddress(), group.GetUploadPrefix());
        UNIT_ASSERT(locations.insert(location).second);
        const auto it = expected.find(location);
        UNIT_ASSERT(it != expected.end());
        UNIT_ASSERT_VALUES_EQUAL(group.GetFiles().size(), it->second.size());
        THashSet<TString> ids;
        for (const auto& file : group.GetFiles()) {
            UNIT_ASSERT(ids.insert(file.GetObjectId()).second);
            UNIT_ASSERT(it->second.contains(file.GetObjectId()));
            UNIT_ASSERT(file.GetLocalPath().empty());
        }
    }
}

void InitTestLogger() {
    NYql::NProto::TLoggingConfig loggerConfig;
    loggerConfig.set_allcomponentslevel(NYql::NProto::TLoggingConfig_ELevel_DEBUG);
    NYql::NLog::InitLogger(loggerConfig, false);
}

NProto::TDqConfig::TScheduler MakeRunningLimiterConfig() {
    NProto::TDqConfig::TScheduler config;
    config.SetLimitRunningTasksPerUserPercent(50);
    return config;
}

TResourceManagerOptions MakeTargetCapacityOption(ui32 maxJobs, i32 workerCapacity = 1) {
    TResourceManagerOptions options;
    options.YtBackend.SetMaxJobs(maxJobs);
    options.YtBackend.SetWorkerCapacity(workerCapacity);
    return options;
}

TIntrusivePtr<TTableNameserverSetup> MakeNameserverTable(const TTestActorRuntimeBase& runtime) {
    auto nameserverTable = MakeIntrusive<TTableNameserverSetup>();
    for (ui32 index = 0; index < runtime.GetNodeCount(); ++index) {
        const auto nodeId = runtime.GetNodeId(index);
        nameserverTable->StaticNodeTable[nodeId] = std::make_pair(ToString(nodeId), nodeId);
    }
    return nameserverTable;
}

class TProbeLock: public TActor<TProbeLock> {
public:
    explicit TProbeLock(TIntrusivePtr<TLockProbe> probe)
        : TActor(&TProbeLock::Main)
        , Probe_(std::move(probe))
    {
    }

    STATEFN(Main) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBecomeFollower, OnBecomeFollower);
            cFunc(TEvents::TEvPoison::EventType, OnPoison);
        }
    }

    void OnBecomeFollower(TEvBecomeFollower::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        Y_UNUSED(ctx);
        ++Probe_->BecomeFollowerCount;
    }

    void OnPoison() {
        ++Probe_->PoisonCount;
        PassAway();
    }

    TIntrusivePtr<TLockProbe> Probe_;
};

class TMockLock: public TActor<TMockLock> {
public:
    TMockLock()
        : TActor(&TMockLock::Main)
    {
    }

    STATEFN(Main) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }
};

class TWarmupUploadLock: public TActor<TWarmupUploadLock> {
public:
    TWarmupUploadLock()
        : TActor(&TWarmupUploadLock::Main)
    {
    }

    TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parent) override {
        return new IEventHandle(parent, self, new TEvBecomeLeader(1, "upload-lock", "{}"));
    }

    STATEFN(Main) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TEvPoison::EventType, PassAway);
        }
    }
};

class TWarmupUploadCoordinator: public TTestCoordinationHelper {
public:
    TWarmupUploadCoordinator(ui32 nodeId, THashMap<TString, TActorId> wrappers)
        : TTestCoordinationHelper(nodeId)
        , Wrappers(std::move(wrappers))
    {
    }

    TActorId GetWrapper(NActors::TActorSystem*, const TString& proxy,
                       const TString&, const TString&, const TString& = {}) override {
        return Wrappers.at(proxy);
    }

    IActor* CreateLockOnCluster(TActorId, const TString&, const TString&, bool) override {
        return new TWarmupUploadLock();
    }

private:
    const THashMap<TString, TActorId> Wrappers;
};

struct TWorkerManagerProbeState: public TThrRefBase {
    ui32 PoisonCount = 0;
};

class TWorkerManagerProbe: public TActor<TWorkerManagerProbe> {
public:
    explicit TWorkerManagerProbe(TIntrusivePtr<TWorkerManagerProbeState> state)
        : TActor(&TWorkerManagerProbe::Main)
        , State_(std::move(state))
    {
    }

    STATEFN(Main) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TEvPoison::EventType, OnPoison);
        }
    }

    void OnPoison() {
        ++State_->PoisonCount;
    }

private:
    TIntrusivePtr<TWorkerManagerProbeState> State_;
};

// Captures TEvAllocateWorkersRequest forwarded to a simulated leader node and relays it
// to an edge actor so a test can assert the forward actually happened.
// `target` is filled in after the runtime is Initialize()-d (edge actors can only be
// allocated post-Initialize, while this probe must be registered pre-Initialize at a
// well-known ActorId) but before any request is sent, so no synchronization is needed.
class TForwardCapture: public TActor<TForwardCapture> {
public:
    explicit TForwardCapture(std::shared_ptr<TActorId> target)
        : TActor(&TForwardCapture::Main)
        , Target_(std::move(target))
    {
    }

    STATEFN(Main) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvAllocateWorkersRequest, OnRequest);
        }
    }

private:
    void OnRequest(TEvAllocateWorkersRequest::TPtr& ev, const TActorContext& ctx) {
        ctx.Send(*Target_, ev->Release().Release());
    }

    std::shared_ptr<TActorId> Target_;
};

}

class TGlobalWorkerManagerTest: public TTestBase {
public:
    UNIT_TEST_SUITE(TGlobalWorkerManagerTest);
    UNIT_TEST(TestTasksAllocation1)
    UNIT_TEST(TestTasksAllocation2)
    UNIT_TEST(TestTasksAllocation3)
    UNIT_TEST(TestTasksAllocation4)
    UNIT_TEST(TestTasksAllocation5)
    UNIT_TEST(RejectsNegativeWorkerCapacity)
    UNIT_TEST(RejectsTargetCapacityOverflow)
    UNIT_TEST_SUITE_END();

    void SetUp() override {
        InitTestLogger();

        // Service node: 0
        // Worker nodes: 1..nodesNumber
        const ui32 nodesNumber = 7;
        ActorRuntime_.Reset(new NActors::TTestActorRuntimeBase(nodesNumber, true));

        // Name server.
        const auto nameserverTable = MakeNameserverTable(*ActorRuntime_);
        ActorRuntime_->AddLocalService(GetNameserviceActorId(),
            TActorSetupCmd(NYql::NDqs::CreateDynamicNameserver(nameserverTable), TMailboxType::Simple, 0));

        // GWM
        const auto uploadResourcesOptions = GetResourceUploaderOptions();
        const TIntrusivePtr<TTestCoordinationHelper> coordinator = new TTestCoordinationHelper(NodeId());
        Sensors_ = MakeIntrusive<TSensorsGroup>();
        const auto gwmActor = CreateGlobalWorkerManager(
            coordinator,
            uploadResourcesOptions,
            CreateMetricsRegistry(Sensors_),
            GetSchedulerConfig());
        ActorRuntime_->AddLocalService(MakeWorkerManagerActorID(NodeId()),
            TActorSetupCmd{gwmActor, TMailboxType::Simple, 0});

        // Local WM.
        FunctionRegistry_ = CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        auto dqCompFactory = NKikimr::NMiniKQL::GetCompositeWithBuiltinFactory({
            NYql::GetCommonDqFactory(),
            NKikimr::NMiniKQL::GetYqlFactory()
        });

        auto dqTaskTransformFactory = NYql::CreateCompositeTaskTransformFactory({
            NYql::CreateCommonDqTaskTransformFactory()
        });

        auto patternCache = std::make_shared<NKikimr::NMiniKQL::TComputationPatternLRUCache>(NKikimr::NMiniKQL::TComputationPatternLRUCache::Config(200_MB, 200_MB));

        auto factory = NTaskRunnerProxy::CreateFactory(FunctionRegistry_.Get(), dqCompFactory, dqTaskTransformFactory, patternCache, true);
        for (ui32 i = 1; i < nodesNumber; i++) {
            NYql::NDqs::TLocalWorkerManagerOptions lwmOptions;
            lwmOptions.TaskRunnerInvokerFactory = new NDqs::TTaskRunnerInvokerFactory();
            lwmOptions.TaskRunnerActorFactory = NYql::NDq::NTaskRunnerActor::CreateTaskRunnerActorFactory(
                lwmOptions.Factory, lwmOptions.TaskRunnerInvokerFactory);
            lwmOptions.FunctionRegistry = FunctionRegistry_.Get();
            lwmOptions.Factory = factory;
            auto localWM = CreateLocalWorkerManager(lwmOptions);
            ActorRuntime_->AddLocalService(MakeWorkerManagerActorID(NodeId(i)),
                TActorSetupCmd{localWM, TMailboxType::Simple, 0}, i);
            ActorRuntime_->AddLocalService(GetNameserviceActorId(),
                TActorSetupCmd(NYql::NDqs::CreateDynamicNameserver(nameserverTable), TMailboxType::Simple, 0), i);
        }

        ActorRuntime_->Initialize();

        for (ui32 i = 1; i < nodesNumber; i++) {
            ActorRuntime_->GetLogSettings(i)->Append(
                NKikimrServices::EServiceKikimr_MIN,
                NKikimrServices::EServiceKikimr_MAX,
                NKikimrServices::EServiceKikimr_Name
            );
            TString explanation;
            auto err = ActorRuntime_->GetLogSettings(i)->SetLevel(NActors::NLog::PRI_EMERG, NKikimrServices::KQP_COMPUTE, explanation); //do not care about CA errors in this test"
            Y_ABORT_IF(err);
        }

        NActors::TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, nodesNumber);
        ActorRuntime_->DispatchEvents(options);

        auto attributes = NYT::BuildYsonNodeFluently()
            .BeginMap()
            .Item(NCommonAttrs::ACTOR_NODEID_ATTR).Value(NodeId())
            .Item(NCommonAttrs::HOSTNAME_ATTR).Value("localhost")
            .Item(NCommonAttrs::GRPCPORT_ATTR).Value("1")
            .Item(NCommonAttrs::UPLOAD_EXECUTABLE_ATTR).Value("true")
            .EndMap();

        // Init GWM.
        auto leaderEv = MakeHolder<TEvBecomeLeader>(1, "1", NYT::NodeToYsonString(attributes));
        const auto dummyActor = ActorRuntime_->AllocateEdgeActor();
        ActorRuntime_->Send(new IEventHandle(MakeWorkerManagerActorID(NodeId()), dummyActor,  leaderEv.Release()));

        auto statusEv = MakeHolder<TEvClusterStatus>();
        const auto statusSender = ActorRuntime_->AllocateEdgeActor();
        ActorRuntime_->Send(new IEventHandle(MakeWorkerManagerActorID(NodeId()), statusSender, statusEv.Release()));
        auto resp = ActorRuntime_->GrabEdgeEvent<TEvClusterStatusResponse>(statusSender);

        // Fake file for GWM.
        TFile outFile("/tmp/test", CreateAlways | ARW);
        outFile.Close();
    }

    void TearDown() override {
        ActorRuntime_.Reset();
        remove("/tmp/test");
    }

    void TestTasksAllocation1() {
        TVector<TVector<TString>> filesPerNode = {
            {"file0", "file11"},
            {"file0", "file22"},
            {"file0", "file33", "file333"},
            {"file0", "file44"},
            {"file0", "file55", "file555"},
            {"file0", "file66", "file6"}
        };

        TVector<TVector<TString>> filesPerTask = {
            {"file333"} // Expect mapping on node 3.
        };

        TVector<ui32> expectedTasksMapping = {3};

        CheckTasksAllocation(filesPerNode, filesPerTask, expectedTasksMapping);
    }

    void TestTasksAllocation2() {
        TVector<TVector<TString>> filesPerNode = {
            {"file0", "file11"},
            {"file0", "file22"},
            {"file0", "file33", "file333"},
            {"file0", "file44"},
            {"file0", "file55", "file555"},
            {"file0", "file66", "file6"}
        };

        TVector<TVector<TString>> filesPerTask = {
            {"file55", "file0"}, // Expect mapping on node 5.
            {"file0", "file11"} // Expect mapping on node 1.
        };

        TVector<ui32> expectedTasksMapping = {5, 1};

        CheckTasksAllocation(filesPerNode, filesPerTask, expectedTasksMapping);
    }

    void TestTasksAllocation3() {
        TVector<TVector<TString>> filesPerNode = {
            {"file0", "file11"},
            {"file0", "file22"},
            {"file0", "file33", "file333"},
            {"file0", "file44"},
            {"file0", "file55", "file555"},
            {"file0", "file66", "file6"}
        };

        TVector<TVector<TString>> filesPerTask = {
            {"file0", "file11"}, // Expect mapping on node 1.
            {"file55", "file0"}, // Expect mapping on node 5.
            {"file44", "file0"} // Expect mapping on node 4.
        };

        TVector<ui32> expectedTasksMapping = {1, 5, 4};

        CheckTasksAllocation(filesPerNode, filesPerTask, expectedTasksMapping);
    }

    void TestTasksAllocation4() {
        TVector<TVector<TString>> filesPerNode = {
            {"file0", "file11"},
            {"file0", "file22"},
            {"file0", "file33", "file333"},
            {"file0", "file44"},
            {"file0", "file55", "file555"},
            {"file0", "file66", "file6"}
        };

        TVector<TVector<TString>> filesPerTask = {
            {"file11"}, // Expect mapping on node 1.
            {"file0", "file333", "file33"}, // Expect mapping on node 3.
            {"file0", "file22"}, // Expect mapping on node 2.
            {"file0", "file555"} // Expect mapping on node 5.
        };

        TVector<ui32> expectedTasksMapping = {1, 3, 2, 5};

        CheckTasksAllocation(filesPerNode, filesPerTask, expectedTasksMapping);
    }

    void TestTasksAllocation5() {
        TVector<TVector<TString>> filesPerNode = {
            {"file0", "file11"},
            {"file0", "file22"},
            {"file0", "file33", "file333"},
            {"file0", "file44"},
            {"file0", "file55", "file555"},
            {"file0", "file66", "file5"}
        };

        TVector<TVector<TString>> filesPerTask = {
            {"file0", "file5", "file66"}, // Expect mapping on node 6.
            {"file0", "file333"}, // Expect mapping on node 3.
            {"file0", "file11"}, // Expect mapping on node 1.
            {"file0", "file44"}, // Expect mapping on node 4.
            {"file0", "file55", "file555"} // Expect mapping on node 5.
        };

        TVector<ui32> expectedTasksMapping = {6, 3, 1, 4, 5};

        CheckTasksAllocation(filesPerNode, filesPerTask, expectedTasksMapping);
    }

    void RejectsNegativeWorkerCapacity() {
        const auto coordinator = MakeIntrusive<TTestCoordinationHelper>(NodeId());
        const auto sensors = MakeIntrusive<TSensorsGroup>();
        auto option = MakeTargetCapacityOption(1U, -1);
        option.YtBackend.SetClusterName("bad-cluster");
        const TVector<TResourceManagerOptions> options = {option};

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            CreateGlobalWorkerManager(
                coordinator,
                options,
                CreateMetricsRegistry(sensors),
                MakeRunningLimiterConfig()),
            yexception,
            "Negative DQ worker capacity -1 configured for cluster bad-cluster");
    }

    void RejectsTargetCapacityOverflow() {
        const auto coordinator = MakeIntrusive<TTestCoordinationHelper>(NodeId());
        const auto sensors = MakeIntrusive<TSensorsGroup>();
        auto option = MakeTargetCapacityOption(Max<ui32>(), Max<i32>());
        option.YtBackend.SetClusterName("overflow-cluster");
        const TVector<TResourceManagerOptions> options = {option, option, option};

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            CreateGlobalWorkerManager(
                coordinator,
                options,
                CreateMetricsRegistry(sensors),
                MakeRunningLimiterConfig()),
            yexception,
            "DQ target capacity overflow while adding cluster overflow-cluster");
    }

    void CheckTasksAllocation(
        const TVector<TVector<TString>>& filesPerNode,
        const TVector<TVector<TString>>& filesPerTask,
        const TVector<ui32>& expectedTasksMapping
    ) {
        Y_ASSERT(expectedTasksMapping.size() == filesPerTask.size());
        Y_ASSERT(filesPerTask.size() <= filesPerNode.size());

        SetupWorkerNodes(filesPerNode);

        const auto execActor = ActorRuntime_->AllocateEdgeActor();
        const auto allocatorActor = RegisterResourceAllocator(filesPerTask.size(), execActor);

        auto allocateRequest = MakeAllocationRequest(filesPerTask);

        ActorRuntime_->Send(new IEventHandle(MakeWorkerManagerActorID(NodeId()), allocatorActor, allocateRequest.Release()));

        auto resp = ActorRuntime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(execActor);

        auto allocatedWorkers = resp->Get()->Record.GetWorkers();

        UNIT_ASSERT_VALUES_EQUAL(filesPerTask.size(), allocatedWorkers.WorkerSize());
        for (ui32 task = 0; task < expectedTasksMapping.size(); ++task) {
            ui32 expectedNode = expectedTasksMapping[task];
            UNIT_ASSERT_VALUES_EQUAL(NodeId(expectedNode), allocatedWorkers.worker(task).GetNodeId());
        }
    }

    TActorId RegisterResourceAllocator(ui32 workersCount, const TActorId& execActor) const {
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto gwmActor = MakeWorkerManagerActorID(NodeId());
        TVector<NYql::NDqProto::TDqTask> tasks(workersCount);
        auto allocator = CreateResourceAllocator(gwmActor, execActor, execActor, workersCount, "TraceId", new TDqConfiguration(), counters, tasks);
        const auto allocatorId = ActorRuntime_->Register(allocator);
        return allocatorId;
    }

    void SetupWorkerNodes(const TVector<TVector<TString>>& filesOnNodes) const {
        ui32 nodeId = 1;
        for (const auto& nodeFiles : filesOnNodes) {
            RegisterNodeInGwm(NodeId(nodeId++), nodeFiles);
        }
    }

    THolder<TEvAllocateWorkersRequest> MakeAllocationRequest(
        const TVector<TVector<TString>>& filesPerTask,
        const TString& traceId = "TraceId",
        const TString& user = "Username") const
    {
        auto allocateRequest = MakeHolder<TEvAllocateWorkersRequest>(filesPerTask.size(), user);
        allocateRequest->Record.SetTraceId(traceId);
        allocateRequest->Record.SetCreateComputeActor(true);
        THashSet<TString> allFiles;

        for (const auto& tf : filesPerTask) {
            *allocateRequest->Record.AddTask() = NYql::NDqProto::TDqTask{};
            Yql::DqsProto::TWorkerFilter taskFiles;
            for (const auto& f : tf) {
                Yql::DqsProto::TFile file;
                file.SetObjectId(f + "_id");
                file.SetLocalPath("/tmp/test");
                *taskFiles.AddFile() = file;
                allFiles.emplace(f);
            }
            *allocateRequest->Record.AddWorkerFilterPerTask() = taskFiles;
        }

        for (const auto& f : allFiles) {
            Yql::DqsProto::TFile file;
            file.SetObjectId(f + "_id");
            file.SetLocalPath("/tmp/test");
            *allocateRequest->Record.AddFiles() = file;
        }

        return allocateRequest;
    }

    ui32 NodeId(ui32 id = 0) const {
        return ActorRuntime_->GetNodeId(id);
    }

    void RegisterNodeInGwm(ui32 nodeId, const TVector<TString>& files) const {
        Yql::DqsProto::RegisterNodeRequest req;
        req.SetCapabilities(Yql::DqsProto::RegisterNodeRequest::ECAP_RUNEXE);
        req.SetNodeId(nodeId);
        req.SetPort(1);
        req.SetRole("worker_node");
        req.SetAddress("Address");
        req.SetRevision(ToString(GetProgramCommitId()));
        req.SetRunningWorkers(1);
        req.SetClusterName("plato");
        req.MutableKnownNodes()->Add(NodeId());
        req.MutableKnownNodes()->Add(nodeId);
        req.MutableGuid()->SetDw0(1);
        req.MutableGuid()->SetDw1(1);
        req.MutableGuid()->SetDw2(1);
        req.MutableGuid()->SetDw3(nodeId);

        for (const auto& file : files) {
            auto fileOnNode = req.AddFilesOnNode();
            fileOnNode->SetObjectId(file + "_id");
        }

        auto ev = MakeHolder<TEvRegisterNode>(req);
        const auto sender = ActorRuntime_->AllocateEdgeActor();
        ActorRuntime_->Send(new IEventHandle(MakeWorkerManagerActorID(NodeId()), sender, ev.Release()));

        ActorRuntime_->GrabEdgeEvent<TEvRegisterNodeResponse>(sender);
    }

protected:
    THolder<NActors::TTestActorRuntimeBase> ActorRuntime_;
    TIntrusivePtr<NKikimr::NMiniKQL::IFunctionRegistry> FunctionRegistry_;
    TIntrusivePtr<TSensorsGroup> Sensors_;

    virtual TVector<TResourceManagerOptions> GetResourceUploaderOptions() const {
        return {};
    }

    virtual NProto::TDqConfig::TScheduler GetSchedulerConfig() const {
        return {};
    }
};

class TGlobalWorkerManagerWarmupTest: public TGlobalWorkerManagerTest {
public:
    UNIT_TEST_SUITE(TGlobalWorkerManagerWarmupTest);
    UNIT_TEST(UsesConfiguredUploadLocationFromServiceNode)
    UNIT_TEST_SUITE_END();

    void UsesConfiguredUploadLocationFromServiceNode() {
        const THashSet<TString> ids = {"udf-one", "udf-two"};
        const auto sender = ActorRuntime_->AllocateEdgeActor();
        Yql::DqsProto::RegisterNodeRequest request;
        request.SetRole("service_node");
        request.SetNodeId(NodeId(1));
        request.MutableGuid()->SetDw0(1);
        request.SetRevision("service-revision");
        for (const auto& id : ids) {
            request.AddFilesOnNode()->SetObjectId(id);
        }
        request.AddFilesOnNode()->SetObjectId("service-revision");
        ActorRuntime_->Send(new IEventHandle(MakeWorkerManagerActorID(NodeId()), sender, new TEvRegisterNode(request)));
        ActorRuntime_->GrabEdgeEvent<TEvRegisterNodeResponse>(sender);

        ui32 workerId = 100;
        for (const auto& options : GetResourceUploaderOptions()) {
            Yql::DqsProto::RegisterNodeRequest worker;
            worker.SetRole("worker_node");
            worker.MutableGuid()->SetDw0(++workerId);
            worker.SetNodeId(NodeId(6) + workerId);
            worker.SetClusterName(options.YtBackend.GetClusterName());
            worker.AddKnownNodes(NodeId());
            ActorRuntime_->Send(new IEventHandle(MakeWorkerManagerActorID(NodeId()), sender, new TEvRegisterNode(worker)));
            const auto event = ActorRuntime_->GrabEdgeEvent<TEvRegisterNodeResponse>(sender);
            const auto& response = event->Get()->Record.GetResponse();
            AssertWarmupGroups(response.GetDownloadGroups(), TWarmupGroups{
                {{options.YtBackend.GetProxyAddress(), options.UploadPrefix}, ids}});
            const auto& legacyFiles = response.GetDownloadList();
            UNIT_ASSERT_VALUES_EQUAL(legacyFiles.size(), ids.size());
            THashSet<TString> legacyIds;
            for (const auto& file : legacyFiles) {
                UNIT_ASSERT(legacyIds.insert(file.GetObjectId()).second);
                UNIT_ASSERT(ids.contains(file.GetObjectId()));
            }
        }
    }

protected:
    TVector<TResourceManagerOptions> GetResourceUploaderOptions() const override {
        TVector<TResourceManagerOptions> result;
        for (const TString cluster : {"cluster-one", "cluster-two"}) {
            auto& options = result.emplace_back();
            options.YtBackend.SetClusterName(cluster);
            options.YtBackend.SetProxyAddress(cluster + ".yt");
            options.YtBackend.SetUploadPrefix("//unused-backend-prefix");
            options.UploadPrefix = "//gwm/" + cluster;
        }
        return result;
    }
};

class TGlobalWorkerManagerRunningLimiterTest: public TGlobalWorkerManagerTest {
public:
    UNIT_TEST_SUITE(TGlobalWorkerManagerRunningLimiterTest);
    UNIT_TEST(RejectsOversizedRequestWithoutRetries);
    UNIT_TEST(UsesTargetCapacityAndQueuesAtLimit);
    UNIT_TEST_SUITE_END();

    void RejectsOversizedRequestWithoutRetries() {
        const auto schedulerCounters = Sensors_->GetSubgroup("component", "scheduler");
        for (const bool workersAvailable : {false, true}) {
            if (workersAvailable) {
                SetupWorkerNodes({{}, {}, {}, {}, {}, {}});
            }

            const auto executer = ActorRuntime_->AllocateEdgeActor();
            const auto allocator = RegisterResourceAllocator(6U, executer);
            auto request = MakeAllocationRequest(TVector<TVector<TString>>(6U));
            auto* file = request->Record.AddFiles();
            file->SetLocalPath("/no/such/file");
            file->SetObjectId("missing-file-id");
            file->SetObjectType(Yql::DqsProto::TFile::EUDF_FILE);
            ActorRuntime_->Send(new IEventHandle(
                MakeWorkerManagerActorID(NodeId()),
                allocator,
                request.Release()));

            const auto response = ActorRuntime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(executer);
            UNIT_ASSERT(response->Get()->Record.HasError());
            const auto& error = response->Get()->Record.GetError();
            UNIT_ASSERT(error.GetStatusCode() == NYql::NDqProto::StatusIds::LIMIT_EXCEEDED);
            UNIT_ASSERT(!NCommon::IsRetriable(error.GetStatusCode()));
            UNIT_ASSERT(NCommon::NeedFallback(error.GetStatusCode()));
            UNIT_ASSERT_STRING_CONTAINS(error.GetMessage(), "Request needs 6 DQ slots");
            UNIT_ASSERT_STRING_CONTAINS(error.GetMessage(), "per-user running limit of 5");
            UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->GetCounter("QueueSizeForLarge")->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->GetCounter("RunningTotal")->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->GetCounter("KnownUsers")->Val(), 0);
        }

        const auto executer = ActorRuntime_->AllocateEdgeActor();
        const auto allocator = RegisterResourceAllocator(5U, executer);
        auto request = MakeAllocationRequest(TVector<TVector<TString>>(5U));
        ActorRuntime_->Send(new IEventHandle(
            MakeWorkerManagerActorID(NodeId()),
            allocator,
            request.Release()));
        const auto response = ActorRuntime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(executer);
        UNIT_ASSERT(!response->Get()->Record.HasError());
    }

    void UsesTargetCapacityAndQueuesAtLimit() {
        SetupWorkerNodes({
            {"file0"},
            {"file0"},
            {"file0"},
            {"file0"},
            {"file0"},
            {"file0"},
        });

        const auto firstExecuter = ActorRuntime_->AllocateEdgeActor();
        const auto firstAllocator = RegisterResourceAllocator(3U, firstExecuter);
        auto firstRequest = MakeAllocationRequest(TVector<TVector<TString>>(3U));
        ActorRuntime_->Send(new IEventHandle(
            MakeWorkerManagerActorID(NodeId()),
            firstAllocator,
            firstRequest.Release()));
        const auto firstResponse = ActorRuntime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(firstExecuter);
        UNIT_ASSERT(!firstResponse->Get()->Record.HasError());

        const auto secondExecuter = ActorRuntime_->AllocateEdgeActor();
        const auto secondAllocator = RegisterResourceAllocator(2U, secondExecuter);
        auto secondRequest = MakeAllocationRequest(TVector<TVector<TString>>(2U), "SecondTraceId");
        ActorRuntime_->Send(new IEventHandle(
            MakeWorkerManagerActorID(NodeId()),
            secondAllocator,
            secondRequest.Release()));
        const auto secondResponse = ActorRuntime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(secondExecuter);
        UNIT_ASSERT(!secondResponse->Get()->Record.HasError());

        const auto queuedExecuter = ActorRuntime_->AllocateEdgeActor();
        const auto queuedAllocator = RegisterResourceAllocator(1U, queuedExecuter);
        auto queuedRequest = MakeAllocationRequest({{}}, "QueuedTraceId");
        ActorRuntime_->Send(new IEventHandle(
            MakeWorkerManagerActorID(NodeId()),
            queuedAllocator,
            queuedRequest.Release()));

        const auto schedulerCounters = Sensors_->GetSubgroup("component", "scheduler");
        ActorRuntime_->WaitFor(
            "request at per-user limit to be queued",
            [&] {
                return schedulerCounters->GetCounter("QueueSizeForSmall")->Val() == 1;
            },
            TDuration::Seconds(5));

        const auto otherExecuter = ActorRuntime_->AllocateEdgeActor();
        const auto otherAllocator = RegisterResourceAllocator(1U, otherExecuter);
        auto otherRequest = MakeAllocationRequest({{}}, "OtherTraceId", "OtherUser");
        ActorRuntime_->Send(new IEventHandle(
            MakeWorkerManagerActorID(NodeId()),
            otherAllocator,
            otherRequest.Release()));
        const auto otherResponse = ActorRuntime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(otherExecuter);
        UNIT_ASSERT(!otherResponse->Get()->Record.HasError());

        ActorRuntime_->Send(new IEventHandle(
            MakeWorkerManagerActorID(NodeId()),
            firstAllocator,
            new TEvFreeWorkersNotify(firstResponse->Get()->Record.GetWorkers().GetResourceId())));
        const auto queuedResponse = ActorRuntime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(queuedExecuter);
        UNIT_ASSERT(!queuedResponse->Get()->Record.HasError());
    }

protected:
    TVector<TResourceManagerOptions> GetResourceUploaderOptions() const override {
        return {MakeTargetCapacityOption(10U)};
    }

    NProto::TDqConfig::TScheduler GetSchedulerConfig() const override {
        return MakeRunningLimiterConfig();
    }
};

class TGlobalWorkerManagerRunningLimiterShrinkTest: public TGlobalWorkerManagerTest {
public:
    UNIT_TEST_SUITE(TGlobalWorkerManagerRunningLimiterShrinkTest);
    UNIT_TEST(QueuesRequestsAboveCurrentCapacity);
    UNIT_TEST_SUITE_END();

    void QueuesRequestsAboveCurrentCapacity() {
        SetupWorkerNodes({
            {},
            {},
            {},
            {},
            {},
            {},
        });

        const auto queuedSender = ActorRuntime_->AllocateEdgeActor();
        ActorRuntime_->Send(new IEventHandle(
            MakeWorkerManagerActorID(NodeId()),
            queuedSender,
            new TEvAllocateWorkersRequest(8U, "QueuedUser")));

        const auto schedulerCounters = Sensors_->GetSubgroup("component", "scheduler");
        ActorRuntime_->WaitFor(
            "request above current capacity to be queued",
            [&] {
                return schedulerCounters->GetCounter("QueueSizeForLarge")->Val() == 1;
            },
            TDuration::Seconds(5));

        const auto secondExecuter = ActorRuntime_->AllocateEdgeActor();
        const auto secondAllocator = RegisterResourceAllocator(8U, secondExecuter);
        auto secondRequest = MakeAllocationRequest(
            TVector<TVector<TString>>(8U),
            "SecondTraceId",
            "SecondUser");
        ActorRuntime_->Send(new IEventHandle(
            MakeWorkerManagerActorID(NodeId()),
            secondAllocator,
            secondRequest.Release()));
        ActorRuntime_->WaitFor(
            "second request above current capacity to be queued",
            [&] {
                return schedulerCounters->GetCounter("QueueSizeForLarge")->Val() == 2;
            },
            TDuration::Seconds(5));

        ActorRuntime_->Send(new IEventHandle(
            secondAllocator,
            ActorRuntime_->AllocateEdgeActor(),
            new TEvents::TEvPoison()));
        ActorRuntime_->WaitFor(
            "cancelled request above current capacity to leave the queue",
            [&] {
                return schedulerCounters->GetCounter("QueueSizeForLarge")->Val() == 1;
            },
            TDuration::Seconds(5));
    }

protected:
    TVector<TResourceManagerOptions> GetResourceUploaderOptions() const override {
        return {
            MakeTargetCapacityOption(4U, 2),
            MakeTargetCapacityOption(6U, 2),
        };
    }

    NProto::TDqConfig::TScheduler GetSchedulerConfig() const override {
        return MakeRunningLimiterConfig();
    }
};

class TGlobalWorkerManagerControlRemovalTest: public TGlobalWorkerManagerTest {
public:
    UNIT_TEST_SUITE(TGlobalWorkerManagerControlRemovalTest);
    UNIT_TEST(OperationStopBypassesAllocationLimits);
    UNIT_TEST(EarlyFreeRemovesOnlyMatchingQueuedRequest);
    UNIT_TEST(TwoEarlyFreesRemoveBothQueuedRequests);
    UNIT_TEST(ZeroIdFreeRemovesAllSenderState);
    UNIT_TEST(UploadFailureRemovesQueuedRequest);
    UNIT_TEST_SUITE_END();

    void OperationStopBypassesAllocationLimits() {
        SetupWorkerNodes({
            {"file0"},
            {"file0"},
            {"file0"},
            {"file0"},
            {"file0"},
            {"file0"},
        });

        const TString stoppedOperationId = "StoppedOperation";
        const auto stoppedOperation = ActorRuntime_->AllocateEdgeActor();
        auto queuedRequest = MakeHolder<TEvAllocateWorkersRequest>(1U, "Username");
        queuedRequest->Record.SetTraceId(stoppedOperationId);
        auto* workerFilter = queuedRequest->Record.AddWorkerFilterPerTask();
        workerFilter->AddFile()->SetObjectId("missing_file");
        ActorRuntime_->Send(new IEventHandle(
            MakeWorkerManagerActorID(NodeId()),
            stoppedOperation,
            queuedRequest.Release()));

        const auto executer = ActorRuntime_->AllocateEdgeActor();
        const auto allocator = RegisterResourceAllocator(2U, executer);
        auto allocationRequest = MakeAllocationRequest(TVector<TVector<TString>>(2U));
        ActorRuntime_->Send(new IEventHandle(
            MakeWorkerManagerActorID(NodeId()),
            allocator,
            allocationRequest.Release()));
        const auto allocationResponse = ActorRuntime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(executer);
        UNIT_ASSERT(!allocationResponse->Get()->Record.HasError());

        const auto schedulerCounters = Sensors_->GetSubgroup("component", "scheduler");
        ActorRuntime_->WaitFor(
            "queued request to appear in metrics",
            [&] {
                return schedulerCounters->GetCounter("QueueSizeForSmall")->Val() == 1;
            },
            TDuration::Seconds(5));

        Yql::DqsProto::OperationStopRequest stopRequest;
        stopRequest.SetOperationId(stoppedOperationId);
        const auto stopSender = ActorRuntime_->AllocateEdgeActor();
        ActorRuntime_->Send(new IEventHandle(
            MakeWorkerManagerActorID(NodeId()),
            stopSender,
            new TEvOperationStop(stopRequest)));

        UNIT_ASSERT(ActorRuntime_->GrabEdgeEvent<TEvOperationStopResponse>(stopSender));
        UNIT_ASSERT(ActorRuntime_->GrabEdgeEvent<TEvDqFailure>(stoppedOperation));
        ActorRuntime_->WaitFor(
            "queued request to be removed",
            [&] {
                return schedulerCounters->GetCounter("QueueSizeForSmall")->Val() == 0;
            },
            TDuration::Seconds(5));
    }

    void EarlyFreeRemovesOnlyMatchingQueuedRequest() {
        SetupWorkerNodes({
            {"file0"},
            {"file0"},
            {"file0"},
            {"file0"},
            {"file0"},
            {"file0"},
        });

        const auto first = QueueUnavailableRequest("FirstTraceId", "first-file");
        const auto second = QueueUnavailableRequest("SecondTraceId", "second-file");

        WaitForSmallQueueSize(2);

        ActorRuntime_->Send(new IEventHandle(
            second,
            ActorRuntime_->AllocateEdgeActor(),
            new TEvents::TEvPoison()));
        WaitForSmallQueueSize(1);
        UNIT_ASSERT_VALUES_EQUAL(GetQueryStatus("SecondTraceId"), "Executing");
        UNIT_ASSERT_VALUES_EQUAL(GetQueryStatus("FirstTraceId"), "Uploading artifacts");

        ActorRuntime_->Send(new IEventHandle(
            first,
            ActorRuntime_->AllocateEdgeActor(),
            new TEvents::TEvPoison()));
        WaitForSmallQueueSize(0);
    }

    void TwoEarlyFreesRemoveBothQueuedRequests() {
        SetupWorkerNodes({
            {"file0"},
            {"file0"},
        });

        const auto first = QueueUnavailableRequest("FirstTraceId", "first-file");
        const auto second = QueueUnavailableRequest("SecondTraceId", "second-file");
        WaitForSmallQueueSize(2);

        ActorRuntime_->Send(new IEventHandle(
            first,
            ActorRuntime_->AllocateEdgeActor(),
            new TEvents::TEvPoison()));
        ActorRuntime_->Send(new IEventHandle(
            second,
            ActorRuntime_->AllocateEdgeActor(),
            new TEvents::TEvPoison()));
        WaitForSmallQueueSize(0);
    }

    void ZeroIdFreeRemovesAllSenderState() {
        SetupWorkerNodes({
            {"file0"},
            {"file0"},
            {"file0"},
            {"file0"},
            {"file0"},
            {"file0"},
        });

        const auto sender = ActorRuntime_->AllocateEdgeActor();
        auto firstRequest = MakeHolder<TEvAllocateWorkersRequest>(1U, "FirstUser");
        firstRequest->Record.SetTraceId("FirstTraceId");
        ActorRuntime_->Send(new IEventHandle(
            MakeWorkerManagerActorID(NodeId()),
            sender,
            firstRequest.Release()));
        const auto firstResponse = ActorRuntime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(sender);
        UNIT_ASSERT(!firstResponse->Get()->Record.HasError());

        auto secondRequest = MakeHolder<TEvAllocateWorkersRequest>(1U, "SecondUser");
        secondRequest->Record.SetTraceId("SecondTraceId");
        ActorRuntime_->Send(new IEventHandle(
            MakeWorkerManagerActorID(NodeId()),
            sender,
            secondRequest.Release()));
        const auto secondResponse = ActorRuntime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(sender);
        UNIT_ASSERT(!secondResponse->Get()->Record.HasError());

        auto queuedRequest = MakeHolder<TEvAllocateWorkersRequest>(1U, "QueuedUser");
        queuedRequest->Record.SetTraceId("QueuedTraceId");
        queuedRequest->Record.AddWorkerFilterPerTask()->AddFile()->SetObjectId("missing-file");
        ActorRuntime_->Send(new IEventHandle(
            MakeWorkerManagerActorID(NodeId()),
            sender,
            queuedRequest.Release()));

        const auto schedulerCounters = Sensors_->GetSubgroup("component", "scheduler");
        const auto runningTotal = schedulerCounters->GetCounter("RunningTotal");
        ActorRuntime_->WaitFor(
            "sender state to be accounted",
            [&] {
                return runningTotal->Val() == 2 &&
                    schedulerCounters->GetCounter("QueueSizeForSmall")->Val() == 1;
            },
            TDuration::Seconds(5));

        ActorRuntime_->Send(new IEventHandle(
            MakeWorkerManagerActorID(NodeId()),
            sender,
            new TEvFreeWorkersNotify(0)));
        ActorRuntime_->WaitFor(
            "zero-id free to remove all sender state",
            [&] {
                return runningTotal->Val() == 0 &&
                    schedulerCounters->GetCounter("QueueSizeForSmall")->Val() == 0;
            },
            TDuration::Seconds(5));
    }

    void UploadFailureRemovesQueuedRequest() {
        SetupWorkerNodes({
            {"file0"},
            {"file0"},
            {"file0"},
            {"file0"},
            {"file0"},
            {"file0"},
        });

        const auto sender = ActorRuntime_->AllocateEdgeActor();
        auto failedRequest = MakeHolder<TEvAllocateWorkersRequest>(3U, "Username");
        failedRequest->Record.SetTraceId("FailedUpload");
        auto* file = failedRequest->Record.AddFiles();
        file->SetLocalPath("/no/such/file");
        file->SetObjectId("missing-file-id");
        file->SetName("missing");
        file->SetObjectType(Yql::DqsProto::TFile::EUDF_FILE);
        ActorRuntime_->Send(new IEventHandle(
            MakeWorkerManagerActorID(NodeId()),
            sender,
            failedRequest.Release()));

        const auto failedResponse = ActorRuntime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(sender);
        UNIT_ASSERT(failedResponse->Get()->Record.HasError());
        UNIT_ASSERT(
            failedResponse->Get()->Record.GetError().GetStatusCode() ==
            NYql::NDqProto::StatusIds::EXTERNAL_ERROR);

        const auto schedulerCounters = Sensors_->GetSubgroup("component", "scheduler");
        UNIT_ASSERT_VALUES_EQUAL(schedulerCounters->GetCounter("QueueSizeForLarge")->Val(), 0);

        const auto executer = ActorRuntime_->AllocateEdgeActor();
        const auto allocator = RegisterResourceAllocator(2U, executer);
        auto allocationRequest = MakeAllocationRequest(TVector<TVector<TString>>(2U));
        ActorRuntime_->Send(new IEventHandle(
            MakeWorkerManagerActorID(NodeId()),
            allocator,
            allocationRequest.Release()));
        const auto allocationResponse = ActorRuntime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(executer);
        UNIT_ASSERT(!allocationResponse->Get()->Record.HasError());
    }

protected:
    TVector<TResourceManagerOptions> GetResourceUploaderOptions() const override {
        return {MakeTargetCapacityOption(6U)};
    }

    NProto::TDqConfig::TScheduler GetSchedulerConfig() const override {
        auto config = MakeRunningLimiterConfig();
        config.SetLimitTasksPerWindow(true);
        config.SetLimiterNumerator(1);
        config.SetLimiterDenumerator(3);
        return config;
    }

private:
    TString GetQueryStatus(const TString& traceId) {
        Yql::DqsProto::QueryStatusRequest request;
        request.SetSession(traceId);
        const auto sender = ActorRuntime_->AllocateEdgeActor();
        ActorRuntime_->Send(new IEventHandle(
            MakeWorkerManagerActorID(NodeId()),
            sender,
            new TEvQueryStatus(request)));
        return ActorRuntime_->GrabEdgeEvent<TEvQueryStatusResponse>(sender)
            ->Get()->Record.GetResponse().GetStatus();
    }

    TActorId QueueUnavailableRequest(const TString& traceId, const TString& fileId) {
        const auto executer = ActorRuntime_->AllocateEdgeActor();
        const auto allocator = RegisterResourceAllocator(1U, executer);
        auto request = MakeHolder<TEvAllocateWorkersRequest>(1U, "Username");
        request->Record.SetTraceId(traceId);
        request->Record.AddWorkerFilterPerTask()->AddFile()->SetObjectId(fileId);
        ActorRuntime_->Send(new IEventHandle(
            MakeWorkerManagerActorID(NodeId()),
            allocator,
            request.Release()));
        return allocator;
    }

    void WaitForSmallQueueSize(i64 expectedSize) {
        const auto queueSize = Sensors_
            ->GetSubgroup("component", "scheduler")
            ->GetCounter("QueueSizeForSmall");
        ActorRuntime_->WaitFor(
            "small queue size to change",
            [&] {
                return queueSize->Val() == expectedSize;
            },
            TDuration::Seconds(5));
    }
};

class TGlobalWorkerManagerStartFollowerTest: public TTestBase {
public:
    UNIT_TEST_SUITE(TGlobalWorkerManagerStartFollowerTest);
    UNIT_TEST(TestJobStopBeforeInitializationReturnsError)
    UNIT_TEST(TestShutdownCookieNotifiesLock)
    UNIT_TEST(TestShutdownCookieIsIdempotent)
    UNIT_TEST(TestShutdownWhileLeaderAllowsAllocate)
    UNIT_TEST(TestRejectBecomeLeaderAfterShutdown)
    UNIT_TEST(TestShutdownKeepsWorkersWhileLockPointsToSelf)
    UNIT_TEST(TestAllocateWaitsForTick)
    UNIT_TEST(TestFollowerLeaderInfoUpdatedAfterShutdown)
    UNIT_TEST(TestFollowerAllocateUploadErrorDoesNotForward)
    UNIT_TEST(TestFollowerRevisionMismatchIsUnsupported)
    UNIT_TEST(TestFollowerForwardedRevisionMismatchWithoutExeFileIsUnsupported)
    UNIT_TEST(TestStopFilterMatchesExistingWorker)
    UNIT_TEST(TestNonForcedStopDoesNotPoisonBusyWorker)
    UNIT_TEST(TestNonForcedStopDoesNotPoisonAllocatedWorkerBeforeHeartbeat)
    UNIT_TEST(TestForcedStopPoisonsBusyWorker)
    UNIT_TEST(TestActiveStopFilterMatchesLateWorker)
    UNIT_TEST(TestStopFilterExpiresAfterRequestedDuration)
    UNIT_TEST(TestStopFilterDurationIsCapped)
    UNIT_TEST(TestNegativeStopFilterDurationIsRejected)
    UNIT_TEST(TestZeroStopFilterDurationUsesLegacyTtl)
    UNIT_TEST(TestZeroStopFilterDurationExpiresAfterLegacyTtl)
    UNIT_TEST_SUITE_END();

    void SetUp() override {
        InitTestLogger();

        LockProbe_ = MakeIntrusive<TLockProbe>();
        Runtime_.Reset(new NActors::TTestActorRuntimeBase(2, false));
        GwmNodeId_ = Runtime_->GetNodeId(0);
        WorkerNodeId_ = Runtime_->GetNodeId(1);

        const auto nameserverTable = MakeNameserverTable(*Runtime_);
        Runtime_->AddLocalService(
            GetNameserviceActorId(),
            TActorSetupCmd(NYql::NDqs::CreateDynamicNameserver(nameserverTable), TMailboxType::Simple, 0));

        WorkerManagerProbe_ = MakeIntrusive<TWorkerManagerProbeState>();
        Runtime_->AddLocalService(
            MakeWorkerManagerActorID(WorkerNodeId_),
            TActorSetupCmd(new TWorkerManagerProbe(WorkerManagerProbe_), TMailboxType::Simple, 0),
            1);

        auto testCoordinationHelper = MakeIntrusive<TTestCoordinationHelper>(GwmNodeId_, LockProbe_);
        CoordinationHelper_ = testCoordinationHelper;
        const TIntrusivePtr<TSensorsGroup> sensorsPtr = new TSensorsGroup();

        Runtime_->Initialize();
        // Prevent periodic ticks; tests inject them explicitly.
        GwmActorId_ = Runtime_->Register(CreateGlobalWorkerManager(
            CoordinationHelper_,
            TVector<TResourceManagerOptions>{TResourceManagerOptions{}},
            CreateMetricsRegistry(sensorsPtr),
            NProto::TDqConfig::TScheduler(),
            /*scheduleInterval*/ TDuration::Seconds(60)));
        PumpEventsUntil([&testCoordinationHelper]() {
            return testCoordinationHelper->CreateLockCalls > 0;
        });

        UNIT_ASSERT_VALUES_EQUAL(1U, testCoordinationHelper->CreateLockCalls);
    }

    void TearDown() override {
        Runtime_.Reset();
        LockProbe_.Reset();
    }

    void TestJobStopBeforeInitializationReturnsError() {
        const auto response = SendJobStop(TDuration::Zero());

        UNIT_ASSERT(response->Get()->Record.GetError().Contains("Waiting for initialization"));
        UNIT_ASSERT(response->Get()->Record.GetRetryable());
    }

    void TestShutdownCookieNotifiesLock() {
        UNIT_ASSERT_VALUES_EQUAL(0U, LockProbe_->BecomeFollowerCount);
        UNIT_ASSERT_VALUES_EQUAL(0U, LockProbe_->PoisonCount);

        SendShutdownCookie();

        UNIT_ASSERT_VALUES_EQUAL(1U, LockProbe_->BecomeFollowerCount);
        UNIT_ASSERT_VALUES_EQUAL(0U, LockProbe_->PoisonCount);
    }

    void TestShutdownCookieIsIdempotent() {
        SendShutdownCookie();
        SendShutdownCookie();

        UNIT_ASSERT_VALUES_EQUAL(1U, LockProbe_->BecomeFollowerCount);
    }

    void TestShutdownWhileLeaderAllowsAllocate() {
        BecomeLeader();
        RegisterWorker(GwmNodeId_ + 1);
        SendShutdownCookie();

        const auto resp = SendAllocateRequest();
        UNIT_ASSERT(!resp->Get()->Record.HasError());
        UNIT_ASSERT_VALUES_EQUAL(1, resp->Get()->Record.GetNodes().WorkerSize());
    }

    void TestRejectBecomeLeaderAfterShutdown() {
        BecomeLeader();
        RegisterWorker(GwmNodeId_ + 1);
        SendShutdownCookie();
        BecomeLeader();

        const auto resp = SendAllocateRequest();
        UNIT_ASSERT(!resp->Get()->Record.HasError());
        UNIT_ASSERT_VALUES_EQUAL(1, resp->Get()->Record.GetNodes().WorkerSize());
    }

    void TestShutdownKeepsWorkersWhileLockPointsToSelf() {
        BecomeLeader();
        RegisterWorker(GwmNodeId_ + 1);
        SendShutdownCookie();
        BecomeFollower("localhost", "1", GwmNodeId_);

        const auto resp = SendAllocateRequest();
        UNIT_ASSERT(!resp->Get()->Record.HasError());
        UNIT_ASSERT_VALUES_EQUAL(1, resp->Get()->Record.GetNodes().WorkerSize());
    }

    void TestAllocateWaitsForTick() {
        BecomeLeader();
        RegisterWorker(GwmNodeId_ + 1);

        const auto sender = SendAllocateRequestAsync();
        WaitForGwmResponse();

        auto pending = Runtime_->CaptureMailboxEvents(sender.Hint(), sender.NodeId());
        UNIT_ASSERT(!NActors::FindEvent<TEvAllocateWorkersResponse>(pending));
        Runtime_->PushMailboxEventsFront(sender.Hint(), sender.NodeId(), pending);

        Runtime_->Send(new IEventHandle(GwmActorId_, TActorId(), new TEvTick()));
        const auto resp = Runtime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(sender);
        UNIT_ASSERT(!resp->Get()->Record.HasError());
        UNIT_ASSERT_VALUES_EQUAL(1, resp->Get()->Record.GetNodes().WorkerSize());
    }

    void TestFollowerLeaderInfoUpdatedAfterShutdown() {
        BecomeFollower("leader-a", "1111", GwmNodeId_ + 1);

        const auto beforeShutdown = SendGetMasterRequest();
        UNIT_ASSERT_VALUES_EQUAL("leader-a", beforeShutdown->Get()->Record.GetResponse().GetHost());
        UNIT_ASSERT_VALUES_EQUAL(1111U, beforeShutdown->Get()->Record.GetResponse().GetPort());

        SendShutdownCookie();
        BecomeFollower("leader-b", "2222", GwmNodeId_ + 2);

        const auto afterUpdate = SendGetMasterRequest();
        UNIT_ASSERT_VALUES_EQUAL("leader-b", afterUpdate->Get()->Record.GetResponse().GetHost());
        UNIT_ASSERT_VALUES_EQUAL(2222U, afterUpdate->Get()->Record.GetResponse().GetPort());
    }

    void TestFollowerAllocateUploadErrorDoesNotForward() {
        BecomeFollower("localhost", "1", GwmNodeId_ + 1);

        const auto sender = Runtime_->AllocateEdgeActor();
        auto request = MakeHolder<TEvAllocateWorkersRequest>(1, "user");
        auto* file = request->Record.AddFiles();
        file->SetLocalPath("/no/such/file");
        file->SetObjectId("missing-file-id");
        file->SetName("missing");
        file->SetObjectType(Yql::DqsProto::TFile::EUDF_FILE);
        // Upload error + revision mismatch: without `return` GWM also sends BAD_REQUEST.

        Runtime_->Send(new IEventHandle(GwmActorId_, sender, request.Release()));
        const auto resp = Runtime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(sender);

        UNIT_ASSERT(resp->Get()->Record.HasError());
        UNIT_ASSERT(resp->Get()->Record.GetError().GetStatusCode() == NYql::NDqProto::StatusIds::EXTERNAL_ERROR);
        UNIT_ASSERT(resp->Get()->Record.GetError().GetMessage().Contains("Unknown file"));

        auto pending = Runtime_->CaptureMailboxEvents(sender.Hint(), sender.NodeId());
        UNIT_ASSERT(!NActors::FindEvent<TEvAllocateWorkersResponse>(pending));
        Runtime_->PushMailboxEventsFront(sender.Hint(), sender.NodeId(), pending);
    }

    void TestFollowerRevisionMismatchIsUnsupported() {
        BecomeFollower("localhost", "1", GwmNodeId_ + 1, "leader-revision");

        const auto resp = SendAllocateRequest();

        UNIT_ASSERT(resp->Get()->Record.HasError());
        UNIT_ASSERT(
            resp->Get()->Record.GetError().GetStatusCode() ==
            NYql::NDqProto::StatusIds::UNSUPPORTED);
        UNIT_ASSERT(resp->Get()->Record.GetError().GetMessage().Contains("Wrong revision"));
    }

    void TestFollowerForwardedRevisionMismatchWithoutExeFileIsUnsupported() {
        BecomeFollower("localhost", "1", GwmNodeId_ + 1, "leader-revision");

        const auto sender = Runtime_->AllocateEdgeActor();
        auto request = MakeHolder<TEvAllocateWorkersRequest>(1, "user");
        request->Record.SetIsForwarded(true);

        Runtime_->Send(new IEventHandle(GwmActorId_, sender, request.Release()));
        const auto resp = Runtime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(sender);

        UNIT_ASSERT(resp->Get()->Record.HasError());
        UNIT_ASSERT(
            resp->Get()->Record.GetError().GetStatusCode() ==
            NYql::NDqProto::StatusIds::UNSUPPORTED);
        UNIT_ASSERT(resp->Get()->Record.GetError().GetMessage().Contains("Wrong revision"));
    }

    void TestActiveStopFilterMatchesLateWorker() {
        BecomeLeader();
        SendJobStop(TDuration::Seconds(30));
        Runtime_->AdvanceCurrentTime(TDuration::Seconds(29));

        RegisterWorker(GwmNodeId_ + 1);

        UNIT_ASSERT(GetOnlyWorker().GetStopping());
    }

    void TestStopFilterMatchesExistingWorker() {
        BecomeLeader();
        RegisterWorker(GwmNodeId_ + 1);

        SendJobStop(TDuration::Seconds(30));

        UNIT_ASSERT(GetOnlyWorker().GetStopping());
    }

    void TestNonForcedStopDoesNotPoisonBusyWorker() {
        BecomeLeader();
        RegisterWorker(WorkerNodeId_);

        SendJobStop(TDuration::Seconds(30));
        SettleWorkerManagerEvents();

        UNIT_ASSERT(GetOnlyWorker().GetStopping());
        UNIT_ASSERT_VALUES_EQUAL(0U, WorkerManagerProbe_->PoisonCount);
    }

    void TestNonForcedStopDoesNotPoisonAllocatedWorkerBeforeHeartbeat() {
        BecomeLeader();
        RegisterWorker(WorkerNodeId_, 0);

        const auto allocateResponse = SendAllocateRequest();
        UNIT_ASSERT(!allocateResponse->Get()->Record.HasError());
        UNIT_ASSERT_VALUES_EQUAL(1, allocateResponse->Get()->Record.GetNodes().WorkerSize());

        SendJobStop(TDuration::Seconds(30));
        SettleWorkerManagerEvents();

        UNIT_ASSERT(GetOnlyWorker().GetStopping());
        UNIT_ASSERT_VALUES_EQUAL(0U, WorkerManagerProbe_->PoisonCount);

        RegisterWorker(WorkerNodeId_, 0);
        SettleWorkerManagerEvents();

        UNIT_ASSERT_VALUES_EQUAL(0U, WorkerManagerProbe_->PoisonCount);
    }

    void TestForcedStopPoisonsBusyWorker() {
        BecomeLeader();
        RegisterWorker(WorkerNodeId_);

        SendJobStop(TDuration::Zero(), true);
        PumpEventsUntil([this]() {
            return WorkerManagerProbe_->PoisonCount > 0;
        });

        UNIT_ASSERT(GetOnlyWorker().GetStopping());
        UNIT_ASSERT_VALUES_EQUAL(1U, WorkerManagerProbe_->PoisonCount);
    }

    void TestStopFilterExpiresAfterRequestedDuration() {
        BecomeLeader();
        SendJobStop(TDuration::Seconds(30));
        Runtime_->AdvanceCurrentTime(TDuration::Seconds(31));

        RegisterWorker(GwmNodeId_ + 1);

        UNIT_ASSERT(!GetOnlyWorker().GetStopping());
    }

    void TestStopFilterDurationIsCapped() {
        BecomeLeader();
        SendJobStop(TDuration::Hours(1));
        Runtime_->AdvanceCurrentTime(TDuration::Minutes(10) + TDuration::Seconds(1));

        RegisterWorker(GwmNodeId_ + 1);

        UNIT_ASSERT(!GetOnlyWorker().GetStopping());
    }

    void TestNegativeStopFilterDurationIsRejected() {
        BecomeLeader();
        const auto response = SendJobStop(-1);

        RegisterWorker(GwmNodeId_ + 1);

        UNIT_ASSERT(response->Get()->Record.GetError().Contains("-1 seconds"));
        UNIT_ASSERT(!response->Get()->Record.GetRetryable());
        UNIT_ASSERT(!GetOnlyWorker().GetStopping());
    }

    void TestZeroStopFilterDurationUsesLegacyTtl() {
        BecomeLeader();
        SendJobStop(TDuration::Zero());
        Runtime_->AdvanceCurrentTime(TDuration::Minutes(9));

        RegisterWorker(GwmNodeId_ + 1);

        UNIT_ASSERT(GetOnlyWorker().GetStopping());
    }

    void TestZeroStopFilterDurationExpiresAfterLegacyTtl() {
        BecomeLeader();
        SendJobStop(TDuration::Zero());
        Runtime_->AdvanceCurrentTime(TDuration::Minutes(10) + TDuration::Seconds(1));

        RegisterWorker(GwmNodeId_ + 1);

        UNIT_ASSERT(!GetOnlyWorker().GetStopping());
    }

private:
    TString MakeLeaderAttributes(
        ui32 leaderNodeId,
        const TString& host,
        const TString& port,
        const TString& revision = {}) const
    {
        auto attributes =
            NYT::BuildYsonNodeFluently()
                .BeginMap()
                    .Item(NCommonAttrs::ACTOR_NODEID_ATTR).Value(leaderNodeId)
                    .Item(NCommonAttrs::HOSTNAME_ATTR).Value(host)
                    .Item(NCommonAttrs::GRPCPORT_ATTR).Value(port)
                .EndMap();
        if (!revision.empty()) {
            attributes[NCommonAttrs::REVISION_ATTR] = revision;
        }
        return NYT::NodeToYsonString(attributes);
    }

    void PumpEventsUntil(const std::function<bool()>& done) {
        if (done()) {
            return;
        }
        NActors::TDispatchOptions options;
        options.CustomFinalCondition = [&done]() {
            return done();
        };
        Runtime_->DispatchEvents(options);
    }

    void WaitForGwmResponse() {
        const auto sender = Runtime_->AllocateEdgeActor();
        Runtime_->Send(new IEventHandle(GwmActorId_, sender, new TEvGetMasterRequest()));
        Runtime_->GrabEdgeEvent<TEvGetMasterResponse>(sender);
    }

    void SettleWorkerManagerEvents() {
        const auto sender = Runtime_->AllocateEdgeActor();
        Runtime_->Schedule(
            new IEventHandle(sender, TActorId(), new TEvents::TEvWakeup()),
            TDuration::Seconds(1));
        Runtime_->GrabEdgeEvent<TEvents::TEvWakeup>(sender);
    }

    void BecomeLeader() {
        Runtime_->Send(new IEventHandle(
            GwmActorId_,
            TActorId(),
            new TEvBecomeLeader(1, "1", MakeLeaderAttributes(GwmNodeId_, "localhost", "1"))));
        WaitForGwmResponse();
    }

    void BecomeFollower(
        const TString& host,
        const TString& port,
        ui32 leaderNodeId,
        const TString& revision = {})
    {
        Runtime_->Send(new IEventHandle(
            GwmActorId_,
            TActorId(),
            new TEvBecomeFollower(MakeLeaderAttributes(leaderNodeId, host, port, revision))));
        WaitForGwmResponse();
    }

    void SendShutdownCookie() {
        Runtime_->Send(new IEventHandle(
            GwmActorId_,
            TActorId(),
            new TEvBecomeFollower(),
            /*flag=*/0,
            /*cookie=*/1));
        PumpEventsUntil([this]() {
            return LockProbe_->BecomeFollowerCount > 0;
        });
    }

    void RegisterWorker(ui32 workerNodeId, int runningWorkers = 1) {
        Yql::DqsProto::RegisterNodeRequest req;
        req.SetCapabilities(Yql::DqsProto::RegisterNodeRequest::ECAP_RUNEXE);
        req.SetNodeId(workerNodeId);
        req.SetPort(1);
        req.SetRole("worker_node");
        req.SetAddress("Address");
        req.SetRevision(ToString(GetProgramCommitId()));
        req.SetRunningWorkers(runningWorkers);
        req.SetClusterName("plato");
        req.MutableKnownNodes()->Add(GwmNodeId_);
        req.MutableKnownNodes()->Add(workerNodeId);
        req.MutableGuid()->SetDw0(1);
        req.MutableGuid()->SetDw1(1);
        req.MutableGuid()->SetDw2(1);
        req.MutableGuid()->SetDw3(workerNodeId);

        const auto sender = Runtime_->AllocateEdgeActor();
        Runtime_->Send(new IEventHandle(GwmActorId_, sender, new TEvRegisterNode(req)));
        Runtime_->GrabEdgeEvent<TEvRegisterNodeResponse>(sender);
    }

    TEvJobStopResponse::TPtr SendJobStop(TDuration duration, bool force = false) {
        return SendJobStop(static_cast<i64>(duration.Seconds()), force);
    }

    TEvJobStopResponse::TPtr SendJobStop(i64 durationSeconds, bool force = false) {
        Yql::DqsProto::JobStopRequest request;
        request.SetRevision("new-revision");
        request.SetNegativeRevision(true);
        request.SetClusterName("plato");
        request.SetTtlSeconds(durationSeconds);
        request.SetForce(force);

        const auto sender = Runtime_->AllocateEdgeActor();
        Runtime_->Send(new IEventHandle(GwmActorId_, sender, new TEvJobStop(request)));
        return Runtime_->GrabEdgeEvent<TEvJobStopResponse>(sender);
    }

    Yql::DqsProto::ClusterStatusResponse::TWorker GetOnlyWorker() {
        const auto sender = Runtime_->AllocateEdgeActor();
        Runtime_->Send(new IEventHandle(GwmActorId_, sender, new TEvClusterStatus()));
        const auto response = Runtime_->GrabEdgeEvent<TEvClusterStatusResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL(1, response->Get()->Record.GetResponse().WorkerSize());
        return response->Get()->Record.GetResponse().GetWorker(0);
    }

    TActorId SendAllocateRequestAsync() {
        const auto sender = Runtime_->AllocateEdgeActor();
        auto request = MakeHolder<TEvAllocateWorkersRequest>(1, "user");
        request->Record.SetTraceId("trace");
        Runtime_->Send(new IEventHandle(GwmActorId_, sender, request.Release()));
        return sender;
    }

    TEvAllocateWorkersResponse::TPtr SendAllocateRequest() {
        const auto sender = SendAllocateRequestAsync();
        Runtime_->Send(new IEventHandle(GwmActorId_, TActorId(), new TEvTick()));
        return Runtime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(sender);
    }

    TEvGetMasterResponse::TPtr SendGetMasterRequest() {
        const auto sender = Runtime_->AllocateEdgeActor();
        Runtime_->Send(new IEventHandle(
            GwmActorId_,
            sender,
            new TEvGetMasterRequest()));
        return Runtime_->GrabEdgeEvent<TEvGetMasterResponse>(sender);
    }

private:
    TIntrusivePtr<TLockProbe> LockProbe_;
    TIntrusivePtr<TWorkerManagerProbeState> WorkerManagerProbe_;
    ICoordinationHelper::TPtr CoordinationHelper_;
    THolder<NActors::TTestActorRuntimeBase> Runtime_;
    TActorId GwmActorId_;
    ui32 GwmNodeId_ = 0;
    ui32 WorkerNodeId_ = 0;
};

// Regression test for the GWM forwarding race: a forwarded TEvAllocateWorkersRequest that
// carries an EEXE_FILE must be forwarded to the (believed) leader even if that GWM's own
// revision differs from the leader's, instead of being rejected with "Wrong revision".
class TGlobalWorkerManagerForwardedExeFileTest: public TTestBase {
public:
    UNIT_TEST_SUITE(TGlobalWorkerManagerForwardedExeFileTest);
    UNIT_TEST(TestForwardedRequestWithExeFileIsForwardedNotRejected)
    UNIT_TEST(TestFollowerAllocateCounters)
    UNIT_TEST_SUITE_END();

    void SetUp() override {
        InitTestLogger();

        LockProbe_ = MakeIntrusive<TLockProbe>();
        Runtime_.Reset(new NActors::TTestActorRuntimeBase(2, false));
        FollowerNodeId_ = Runtime_->GetNodeId(0);
        LeaderNodeId_ = Runtime_->GetNodeId(1);

        const auto nameserverTable = MakeNameserverTable(*Runtime_);
        Runtime_->AddLocalService(
            GetNameserviceActorId(),
            TActorSetupCmd(NYql::NDqs::CreateDynamicNameserver(nameserverTable), TMailboxType::Simple, 0),
            0);
        Runtime_->AddLocalService(
            GetNameserviceActorId(),
            TActorSetupCmd(NYql::NDqs::CreateDynamicNameserver(nameserverTable), TMailboxType::Simple, 0),
            1);

        auto testCoordinationHelper = MakeIntrusive<TTestCoordinationHelper>(FollowerNodeId_, LockProbe_);
        CoordinationHelper_ = testCoordinationHelper;
        Sensors_ = MakeIntrusive<TSensorsGroup>();

        CaptureTarget_ = std::make_shared<TActorId>();
        Runtime_->AddLocalService(
            MakeWorkerManagerActorID(LeaderNodeId_),
            TActorSetupCmd{new TForwardCapture(CaptureTarget_), TMailboxType::Simple, 0},
            1);

        Runtime_->Initialize();

        GwmActorId_ = Runtime_->Register(CreateGlobalWorkerManager(
            CoordinationHelper_,
            {},
            CreateMetricsRegistry(Sensors_),
            NProto::TDqConfig::TScheduler()));

        CaptureEdge_ = Runtime_->AllocateEdgeActor(1);
        *CaptureTarget_ = CaptureEdge_;

        NActors::TDispatchOptions options;
        options.CustomFinalCondition = [&testCoordinationHelper]() {
            return testCoordinationHelper->CreateLockCalls > 0;
        };
        Runtime_->DispatchEvents(options);

        // Point the follower at LeaderNodeId_ with a deliberately mismatched revision.
        auto attributes =
            NYT::BuildYsonNodeFluently()
                .BeginMap()
                    .Item(NCommonAttrs::ACTOR_NODEID_ATTR).Value(LeaderNodeId_)
                    .Item(NCommonAttrs::HOSTNAME_ATTR).Value("localhost")
                    .Item(NCommonAttrs::GRPCPORT_ATTR).Value("1")
                    .Item(NCommonAttrs::REVISION_ATTR).Value("leader-revision")
                .EndMap();
        Runtime_->Send(new IEventHandle(
            GwmActorId_,
            TActorId(),
            new TEvBecomeFollower(NYT::NodeToYsonString(attributes))));

        const auto masterSender = Runtime_->AllocateEdgeActor();
        Runtime_->Send(new IEventHandle(GwmActorId_, masterSender, new TEvGetMasterRequest()));
        Runtime_->GrabEdgeEvent<TEvGetMasterResponse>(masterSender);
    }

    void TearDown() override {
        Runtime_.Reset();
    }

    void TestForwardedRequestWithExeFileIsForwardedNotRejected() {
        const auto sender = Runtime_->AllocateEdgeActor();
        auto request = MakeHolder<TEvAllocateWorkersRequest>(1, "user");
        request->Record.SetIsForwarded(true);
        auto* file = request->Record.AddFiles();
        file->SetObjectType(Yql::DqsProto::TFile::EEXE_FILE);
        file->SetName("dq_vanilla_job.lite");
        file->SetObjectId("exe-object-id");
        request->Record.SetUploadPrefix("//client/resources");
        request->Record.SetUploadClusterAddress("client-cluster.yt");

        Runtime_->Send(new IEventHandle(GwmActorId_, sender, request.Release()));

        // The request must reach the (believed) leader instead of being rejected.
        const auto forwarded = Runtime_->GrabEdgeEvent<TEvAllocateWorkersRequest>(CaptureEdge_);
        const auto& files = forwarded->Get()->Record.GetFiles();
        UNIT_ASSERT_VALUES_EQUAL(1, files.size());
        UNIT_ASSERT(files.begin()->GetObjectType() == Yql::DqsProto::TFile::EEXE_FILE);
        UNIT_ASSERT_VALUES_EQUAL(forwarded->Get()->Record.GetUploadPrefix(), "//client/resources");
        UNIT_ASSERT_VALUES_EQUAL(forwarded->Get()->Record.GetUploadClusterAddress(), "client-cluster.yt");
    }

    void TestFollowerAllocateCounters() {
        auto requestCounters = Sensors_
            ->GetSubgroup("counters", "gwm")
            ->GetSubgroup("component", "requests");
        auto withExeFileCounter = requestCounters->FindCounter("AllocateWorkersWithExeFile");
        auto withoutExeFileCounter = requestCounters->FindCounter("AllocateWorkersWithoutExeFile");

        UNIT_ASSERT(withExeFileCounter);
        UNIT_ASSERT(withoutExeFileCounter);
        UNIT_ASSERT(withExeFileCounter->ForDerivative());
        UNIT_ASSERT(withoutExeFileCounter->ForDerivative());
        UNIT_ASSERT_VALUES_EQUAL(0, withExeFileCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, withoutExeFileCounter->Val());

        const auto withExeFileSender = Runtime_->AllocateEdgeActor();
        auto withExeFileRequest = MakeHolder<TEvAllocateWorkersRequest>(1, "user");
        withExeFileRequest->Record.SetIsForwarded(true);
        auto* exeFile = withExeFileRequest->Record.AddFiles();
        exeFile->SetObjectType(Yql::DqsProto::TFile::EEXE_FILE);
        exeFile->SetName("dq_vanilla_job.lite");
        exeFile->SetObjectId("exe-object-id");
        Runtime_->Send(new IEventHandle(GwmActorId_, withExeFileSender, withExeFileRequest.Release()));
        Runtime_->GrabEdgeEvent<TEvAllocateWorkersRequest>(CaptureEdge_);

        const auto withoutExeFileSender = Runtime_->AllocateEdgeActor();
        auto withoutExeFileRequest = MakeHolder<TEvAllocateWorkersRequest>(1, "user");
        Runtime_->Send(new IEventHandle(GwmActorId_, withoutExeFileSender, withoutExeFileRequest.Release()));
        Runtime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(withoutExeFileSender);

        // Failed uploads must not affect either counter.
        const auto uploadErrorSender = Runtime_->AllocateEdgeActor();
        auto uploadErrorRequest = MakeHolder<TEvAllocateWorkersRequest>(1, "user");
        auto* missingFile = uploadErrorRequest->Record.AddFiles();
        missingFile->SetLocalPath("/no/such/file");
        missingFile->SetObjectId("missing-file-id");
        missingFile->SetName("missing");
        missingFile->SetObjectType(Yql::DqsProto::TFile::EUDF_FILE);
        Runtime_->Send(new IEventHandle(GwmActorId_, uploadErrorSender, uploadErrorRequest.Release()));
        Runtime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(uploadErrorSender);

        UNIT_ASSERT_VALUES_EQUAL(1, withExeFileCounter->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, withoutExeFileCounter->Val());
    }

private:
    TIntrusivePtr<TLockProbe> LockProbe_;
    ICoordinationHelper::TPtr CoordinationHelper_;
    THolder<NActors::TTestActorRuntimeBase> Runtime_;
    TIntrusivePtr<TSensorsGroup> Sensors_;
    TActorId GwmActorId_;
    TActorId CaptureEdge_;
    std::shared_ptr<TActorId> CaptureTarget_;
    ui32 FollowerNodeId_ = 0;
    ui32 LeaderNodeId_ = 0;
};

class TGlobalWorkerManagerQueueProcessingTest: public TTestBase {
public:
    UNIT_TEST_SUITE(TGlobalWorkerManagerQueueProcessingTest);
    UNIT_TEST(TestProcessesReadyQueueInOneTick)
    UNIT_TEST(TestReachesReadyRequestAfterBlockedPrefix)
    UNIT_TEST(CountsWorkerLossAndNoWorkers)
    UNIT_TEST_SUITE_END();

    // Exceeds the removed 200-request per-tick cap, so one tick must drain the whole queue.
    static constexpr size_t QueuedRequests = 250;

    void SetUp() override {
        Runtime_.Reset(new NActors::TTestActorRuntimeBase(1, false));
        GwmNodeId_ = Runtime_->GetNodeId(0);

        NameserverTable_ = MakeNameserverTable(*Runtime_);
        Runtime_->AddLocalService(
            GetNameserviceActorId(),
            NActors::TActorSetupCmd(
                NYql::NDqs::CreateDynamicNameserver(NameserverTable_),
                NActors::TMailboxType::Simple,
                0));

        Coordinator_ = MakeIntrusive<TTestCoordinationHelper>(GwmNodeId_);
        Sensors_ = MakeIntrusive<TSensorsGroup>();

        Runtime_->Initialize();
        GwmActorId_ = Runtime_->Register(CreateGlobalWorkerManager(
            Coordinator_,
            {},
            CreateMetricsRegistry(Sensors_),
            CreateSchedulerConfig()));
        Runtime_->SetScheduledEventsSelectorFunc(NActors::TTestActorRuntimeBase::DroppingScheduledEventsSelector);

        NActors::TDispatchOptions bootOpts;
        bootOpts.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, 1);
        Runtime_->DispatchEvents(bootOpts);

        BecomeLeader();
        RegisterWorker(/*withFile=*/false);

        TFile outFile("/tmp/test", CreateAlways | ARW);
        outFile.Close();
    }

    void TearDown() override {
        Runtime_.Reset();
        remove("/tmp/test");
    }

    void TestProcessesReadyQueueInOneTick() {
        const TActorId collector = Runtime_->AllocateEdgeActor();

        for (size_t i = 0; i < QueuedRequests; ++i) {
            Runtime_->Send(new IEventHandle(
                GwmActorId_,
                collector,
                MakeAllocateRequest(i).Release()));
        }

        UNIT_ASSERT_VALUES_EQUAL(QueuedRequests, GetWaitListSize());

        RegisterWorker(/*withFile=*/true);
        Runtime_->Send(new IEventHandle(GwmActorId_, TActorId(), new TEvTick()));

        for (size_t i = 0; i < QueuedRequests; ++i) {
            auto resp = Runtime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(collector, TDuration::Seconds(5));
            UNIT_ASSERT_C(resp, "Expected allocation response " << i);
        }

        UNIT_ASSERT_VALUES_EQUAL(0u, GetWaitListSize());
    }

    void TestReachesReadyRequestAfterBlockedPrefix() {
        const TActorId collector = Runtime_->AllocateEdgeActor();

        for (size_t i = 0; i < QueuedRequests; ++i) {
            Runtime_->Send(new IEventHandle(
                GwmActorId_,
                collector,
                MakeAllocateRequest(i).Release()));
        }
        Runtime_->Send(new IEventHandle(
            GwmActorId_,
            collector,
            MakeAllocateRequest(QueuedRequests, "ready_file_id").Release()));

        UNIT_ASSERT_VALUES_EQUAL(QueuedRequests + 1, GetWaitListSize());

        Runtime_->Send(new IEventHandle(GwmActorId_, TActorId(), new TEvTick()));

        auto resp = Runtime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(collector, TDuration::Seconds(5));
        UNIT_ASSERT_C(resp, "Expected allocation response for ready request");
        UNIT_ASSERT_VALUES_EQUAL(QueuedRequests, GetWaitListSize());
    }

    void CountsWorkerLossAndNoWorkers() {
        // Clear the schedule left by worker registration before testing worker loss.
        Runtime_->Send(new IEventHandle(GwmActorId_, TActorId(), new TEvTick()));
        UNIT_ASSERT_VALUES_EQUAL(0u, GetWaitListSize());

        const auto sender = Runtime_->AllocateEdgeActor();
        Runtime_->Send(new IEventHandle(
            GwmActorId_,
            sender,
            MakeAllocateRequest(/*index*/ 0).Release()));
        UNIT_ASSERT_VALUES_EQUAL(1u, GetWaitListSize());

        RegisterWorker(/*withFile=*/ false, /*zombie=*/ true);
        Runtime_->Send(new IEventHandle(GwmActorId_, TActorId(), new TEvTick()));
        const auto waitingResponse = Runtime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(sender);
        UNIT_ASSERT(
            waitingResponse->Get()->Record.GetError().GetStatusCode() ==
            NYql::NDqProto::StatusIds::OVERLOADED);
        UNIT_ASSERT_VALUES_EQUAL(
            "All DQ workers became unavailable while the operation was waiting",
            waitingResponse->Get()->Record.GetError().GetMessage());

        Runtime_->Send(new IEventHandle(
            GwmActorId_,
            sender,
            MakeAllocateRequest(/*index*/ 1).Release()));
        const auto newResponse = Runtime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(sender);
        UNIT_ASSERT(
            newResponse->Get()->Record.GetError().GetStatusCode() ==
            NYql::NDqProto::StatusIds::OVERLOADED);
        UNIT_ASSERT_VALUES_EQUAL(
            "No DQ workers are available to run the operation",
            newResponse->Get()->Record.GetError().GetMessage());

        const auto overloadCounters = Sensors_
            ->GetSubgroup("counters", "gwm")
            ->FindSubgroup("component", "overload");
        UNIT_ASSERT(overloadCounters);
        UNIT_ASSERT_VALUES_EQUAL(2, overloadCounters->FindCounter("Total")->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, overloadCounters->FindCounter("WorkersLost")->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, overloadCounters->FindCounter("NoWorkers")->Val());
    }

    void WaitingOperationLimits() {
        const auto sender = Runtime_->AllocateEdgeActor();

        Runtime_->Send(new IEventHandle(
            GwmActorId_,
            sender,
            MakeAllocateRequest(/*index*/ 0, "file0_id", /*count*/ 2, "user").Release()));
        UNIT_ASSERT_VALUES_EQUAL(1u, GetWaitListSize());

        Runtime_->Send(new IEventHandle(
            GwmActorId_,
            sender,
            MakeAllocateRequest(/*index*/ 1, "file0_id", /*count*/ 2, "user").Release()));
        auto perUserResponse = Runtime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(sender);
        UNIT_ASSERT(
            perUserResponse->Get()->Record.GetError().GetStatusCode() ==
            NYql::NDqProto::StatusIds::OVERLOADED);
        UNIT_ASSERT_VALUES_EQUAL(
            "Too many dq operations: per-user queue limit reached (waiting: 1, limit: 1)",
            perUserResponse->Get()->Record.GetError().GetMessage());

        Runtime_->Send(new IEventHandle(
            GwmActorId_,
            sender,
            MakeAllocateRequest(/*index*/ 2, "file0_id", /*count*/ 2, "other-user").Release()));
        auto globalResponse = Runtime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(sender);
        UNIT_ASSERT(
            globalResponse->Get()->Record.GetError().GetStatusCode() ==
            NYql::NDqProto::StatusIds::OVERLOADED);
        UNIT_ASSERT_VALUES_EQUAL(
            "Too many dq operations: global large-operation queue limit reached (waiting: 1, limit: 1)",
            globalResponse->Get()->Record.GetError().GetMessage());

        const auto gwmCounters = Sensors_->FindSubgroup("counters", "gwm");
        UNIT_ASSERT(gwmCounters);
        const auto overloadCounters = gwmCounters->FindSubgroup("component", "overload");
        UNIT_ASSERT(overloadCounters);
        UNIT_ASSERT_VALUES_EQUAL(2, overloadCounters->FindCounter("Total")->Val());
    }

protected:
    virtual NProto::TDqConfig::TScheduler CreateSchedulerConfig() const
    {
        return {};
    }

private:
    TString MakeLeaderAttributes() const {
        return NYT::NodeToYsonString(
            NYT::BuildYsonNodeFluently()
                .BeginMap()
                    .Item(NCommonAttrs::ACTOR_NODEID_ATTR).Value(GwmNodeId_)
                    .Item(NCommonAttrs::HOSTNAME_ATTR).Value("localhost")
                    .Item(NCommonAttrs::GRPCPORT_ATTR).Value("1")
                    .Item(NCommonAttrs::REVISION_ATTR).Value(ToString(GetProgramCommitId()))
                    .Item(NCommonAttrs::UPLOAD_EXECUTABLE_ATTR).Value("true")
                .EndMap());
    }

    void BecomeLeader() {
        Runtime_->Send(new IEventHandle(
            GwmActorId_,
            TActorId(),
            new TEvBecomeLeader(1, "1", MakeLeaderAttributes())));

        const TActorId statusSender = Runtime_->AllocateEdgeActor();
        Runtime_->Send(new IEventHandle(GwmActorId_, statusSender, new TEvClusterStatus()));
        Runtime_->GrabEdgeEvent<TEvClusterStatusResponse>(statusSender);
    }

    void RegisterWorker(bool withFile = true, bool zombie = false) {
        Yql::DqsProto::RegisterNodeRequest req;
        req.SetCapabilities(Yql::DqsProto::RegisterNodeRequest::ECAP_RUNEXE);
        req.SetNodeId(GwmNodeId_ + 1);
        req.SetPort(1);
        req.SetRole("worker_node");
        req.SetAddress("Address");
        req.SetRevision(ToString(GetProgramCommitId()));
        req.SetRunningWorkers(0);
        req.SetCapacity(400);
        req.SetZombie(zombie);
        req.SetClusterName("plato");
        req.MutableKnownNodes()->Add(GwmNodeId_);
        req.MutableKnownNodes()->Add(GwmNodeId_ + 1);
        req.MutableGuid()->SetDw0(1);
        req.MutableGuid()->SetDw1(1);
        req.MutableGuid()->SetDw2(1);
        req.MutableGuid()->SetDw3(GwmNodeId_ + 1);
        req.AddFilesOnNode()->SetObjectId("ready_file_id");
        if (withFile) {
            req.AddFilesOnNode()->SetObjectId("file0_id");
        }

        const TActorId sender = Runtime_->AllocateEdgeActor();
        Runtime_->Send(new IEventHandle(GwmActorId_, sender, new TEvRegisterNode(req)));
        Runtime_->GrabEdgeEvent<TEvRegisterNodeResponse>(sender);
    }

    THolder<TEvAllocateWorkersRequest> MakeAllocateRequest(
        size_t index,
        const TString& objectId = "file0_id",
        ui32 count = 1,
        const TString& user = "user") const
    {
        auto request = MakeHolder<TEvAllocateWorkersRequest>(count, user);
        request->Record.SetTraceId("trace-" + ToString(index));
        request->Record.SetCreateComputeActor(false);

        Yql::DqsProto::TFile file;
        file.SetObjectId(objectId);
        file.SetLocalPath("/tmp/test");
        *request->Record.AddFiles() = file;

        Yql::DqsProto::TWorkerFilter filter;
        *filter.AddFile() = file;
        for (ui32 i = 0; i < count; ++i) {
            *request->Record.AddWorkerFilterPerTask() = filter;
        }

        return request;
    }

    size_t GetWaitListSize() {
        const TActorId statusSender = Runtime_->AllocateEdgeActor();
        Runtime_->Send(new IEventHandle(GwmActorId_, statusSender, new TEvClusterStatus()));
        auto resp = Runtime_->GrabEdgeEvent<TEvClusterStatusResponse>(statusSender, TDuration::Seconds(5));
        UNIT_ASSERT_C(resp, "TEvClusterStatusResponse timeout");
        return resp->Get()->Record.GetResponse().WaitListSize();
    }

private:
    TIntrusivePtr<TTestCoordinationHelper> Coordinator_;
    TIntrusivePtr<TSensorsGroup> Sensors_;
    TIntrusivePtr<NActors::TTableNameserverSetup> NameserverTable_;
    THolder<NActors::TTestActorRuntimeBase> Runtime_;
    TActorId GwmActorId_;
    ui32 GwmNodeId_ = 0;
};

class TGlobalWorkerManagerQueueLimitsTest
    : public TGlobalWorkerManagerQueueProcessingTest
{
public:
    UNIT_TEST_SUITE(TGlobalWorkerManagerQueueLimitsTest);
    UNIT_TEST(WaitingOperationLimits)
    UNIT_TEST_SUITE_END();

protected:
    NProto::TDqConfig::TScheduler CreateSchedulerConfig() const override
    {
        NProto::TDqConfig::TScheduler config;
        config.SetMaxOperations(1);
        config.SetMaxOperationsPerUser(1);
        return config;
    }
};

class TGlobalWorkerManagerDisconnectTest: public TGlobalWorkerManagerTest {
public:
    UNIT_TEST_SUITE(TGlobalWorkerManagerDisconnectTest);
    UNIT_TEST(TestPerUserLimitAfterNodeDisconnect);
    UNIT_TEST_SUITE_END();

    void SetUp() override {
        InitTestLogger();
        const ui32 nodesNumber = 4;
        ActorRuntime_.Reset(new NActors::TTestActorRuntimeBase(nodesNumber, true));
        const auto nameserverTable = MakeNameserverTable(*ActorRuntime_);
        ActorRuntime_->AddLocalService(GetNameserviceActorId(),
            TActorSetupCmd(NYql::NDqs::CreateDynamicNameserver(nameserverTable), TMailboxType::Simple, 0));
        TVector<TResourceManagerOptions> uploadResourcesOptions;
        const TIntrusivePtr<TTestCoordinationHelper> coordPtr = new TTestCoordinationHelper(NodeId());
        const TIntrusivePtr<TSensorsGroup> sensorsPtr = new TSensorsGroup();
        NProto::TDqConfig::TScheduler schedulerCfg;
        schedulerCfg.SetMaxOperationsPerUser(5);
        schedulerCfg.SetMaxOperations(1000);
        // The worker is pinged once in SetUp, so with the default tick CleanUp would cross the
        // 15s stale threshold mid-test and answer OVERLOADED. Keep it from firing at all.
        const auto gwmActor = CreateGlobalWorkerManager(
            coordPtr, uploadResourcesOptions, CreateMetricsRegistry(sensorsPtr), schedulerCfg,
            TDuration::Seconds(60));
        ActorRuntime_->AddLocalService(MakeWorkerManagerActorID(NodeId()),
            TActorSetupCmd{gwmActor, TMailboxType::Simple, 0});
        FunctionRegistry_ = CreateFunctionRegistry(NKikimr::NMiniKQL::CreateBuiltinRegistry());
        auto dqCompFactory = NKikimr::NMiniKQL::GetCompositeWithBuiltinFactory({
            NYql::GetCommonDqFactory(),
            NKikimr::NMiniKQL::GetYqlFactory()
        });
        auto dqTaskTransformFactory = NYql::CreateCompositeTaskTransformFactory({
            NYql::CreateCommonDqTaskTransformFactory()
        });
        auto patternCache = std::make_shared<NKikimr::NMiniKQL::TComputationPatternLRUCache>(
            NKikimr::NMiniKQL::TComputationPatternLRUCache::Config(200_MB, 200_MB));
        auto factory = NTaskRunnerProxy::CreateFactory(
            FunctionRegistry_.Get(), dqCompFactory, dqTaskTransformFactory, patternCache, true);
        for (ui32 i = 1; i < nodesNumber; i++) {
            NYql::NDqs::TLocalWorkerManagerOptions lwmOptions;
            lwmOptions.TaskRunnerInvokerFactory = new NDqs::TTaskRunnerInvokerFactory();
            lwmOptions.TaskRunnerActorFactory = NYql::NDq::NTaskRunnerActor::CreateTaskRunnerActorFactory(
                lwmOptions.Factory, lwmOptions.TaskRunnerInvokerFactory);
            lwmOptions.FunctionRegistry = FunctionRegistry_.Get();
            lwmOptions.Factory = factory;
            auto localWM = CreateLocalWorkerManager(lwmOptions);
            ActorRuntime_->AddLocalService(MakeWorkerManagerActorID(NodeId(i)),
                TActorSetupCmd{localWM, TMailboxType::Simple, 0}, i);
            ActorRuntime_->AddLocalService(GetNameserviceActorId(),
                TActorSetupCmd(NYql::NDqs::CreateDynamicNameserver(nameserverTable), TMailboxType::Simple, 0), i);
        }
        ActorRuntime_->Initialize();
        NActors::TDispatchOptions options;
        options.FinalEvents.emplace_back(NActors::TEvents::TSystem::Bootstrap, nodesNumber);
        ActorRuntime_->DispatchEvents(options);
        auto attributes = NYT::BuildYsonNodeFluently()
            .BeginMap()
            .Item(NCommonAttrs::ACTOR_NODEID_ATTR).Value(NodeId())
            .Item(NCommonAttrs::HOSTNAME_ATTR).Value("localhost")
            .Item(NCommonAttrs::GRPCPORT_ATTR).Value("1")
            .Item(NCommonAttrs::UPLOAD_EXECUTABLE_ATTR).Value("true")
            .EndMap();
        auto leaderEv = MakeHolder<TEvBecomeLeader>(1, "1", NYT::NodeToYsonString(attributes));
        const auto dummyActor = ActorRuntime_->AllocateEdgeActor();
        ActorRuntime_->Send(new IEventHandle(MakeWorkerManagerActorID(NodeId()), dummyActor, leaderEv.Release()));
        TFile outFile("/tmp/test", CreateAlways | ARW);
        outFile.Close();
    }

    void TestPerUserLimitAfterNodeDisconnect() {
        SetupWorkerNodes({{"file0"}});
        const ui32 allocatorNodeIndex = 2;
        const ui32 allocatorNodeId = NodeId(allocatorNodeIndex);
        const auto execActor = ActorRuntime_->AllocateEdgeActor(allocatorNodeIndex);
        const auto allocatorActor = RegisterResourceAllocatorOnNode(5, execActor, allocatorNodeIndex);
        TVector<TVector<TString>> blockedTask(5, {"file0"});
        auto blockedRequest = MakeAllocationRequest(blockedTask, "BlockedTraceId");
        ActorRuntime_->Send(new IEventHandle(
            MakeWorkerManagerActorID(NodeId()), allocatorActor, blockedRequest.Release(), 0, allocatorNodeId));
        ActorRuntime_->Send(new IEventHandle(
            MakeWorkerManagerActorID(NodeId()),
            MakeWorkerManagerActorID(NodeId()),
            new TEvInterconnect::TEvNodeDisconnected(allocatorNodeId)));
        // After the disconnect, the per-user operation slot must be freed. Send 5 new allocation
        // requests (MaxOperationsPerUser == 5) with unique TraceIds: they cannot be satisfied (only
        // one worker node, not enough capacity for 5 workers), so they stay suspended in the
        // scheduler. We do not wait for responses here because none arrive until workers become
        // available.
        for (ui32 i = 0; i < 5; ++i) {
            const auto nextExecActor = ActorRuntime_->AllocateEdgeActor(allocatorNodeIndex + 1);
            const auto nextAllocator = RegisterResourceAllocatorOnNode(5, nextExecActor, allocatorNodeIndex + 1);
            TVector<TVector<TString>> task(5, {"file0"});
            auto request = MakeAllocationRequest(task, TStringBuilder() << "TraceId_" << i);
            ActorRuntime_->Send(new IEventHandle(
                MakeWorkerManagerActorID(NodeId()), nextAllocator, request.Release()));
        }

        // Each of the five requests must be queued; a leaked per-user slot would reject one.
        const auto queryActor = ActorRuntime_->AllocateEdgeActor();
        for (ui32 i = 0; i < 5; ++i) {
            Yql::DqsProto::QueryStatusRequest statusReq;
            statusReq.SetSession(TStringBuilder() << "TraceId_" << i);
            ActorRuntime_->Send(new IEventHandle(
                MakeWorkerManagerActorID(NodeId()), queryActor, new TEvQueryStatus(statusReq)));
            auto resp = ActorRuntime_->GrabEdgeEvent<TEvQueryStatusResponse>(queryActor);
            UNIT_ASSERT_C(
                resp->Get()->Record.GetResponse().GetStatus() == "Awaiting",
                TStringBuilder() << "Request " << i << " (TraceId_" << i
                    << ") expected to be queued (Awaiting) but got: "
                    << resp->Get()->Record.GetResponse().GetStatus());
        }

        {
            const auto overflowExecActor = ActorRuntime_->AllocateEdgeActor(allocatorNodeIndex + 1);
            const auto overflowAllocator = RegisterResourceAllocatorOnNode(5, overflowExecActor, allocatorNodeIndex + 1);
            TVector<TVector<TString>> task(5, {"file0"});
            auto request = MakeAllocationRequest(task, "OverflowTraceId");
            ActorRuntime_->Send(new IEventHandle(
                MakeWorkerManagerActorID(NodeId()), overflowAllocator, request.Release()));
            auto resp = ActorRuntime_->GrabEdgeEvent<TEvAllocateWorkersResponse>(overflowExecActor);
            UNIT_ASSERT_EQUAL(
                resp->Get()->Record.GetTResponseCase(),
                NYql::NDqProto::TAllocateWorkersResponse::kError);
            UNIT_ASSERT_EQUAL(
                resp->Get()->Record.GetError().GetStatusCode(),
                NYql::NDqProto::StatusIds::OVERLOADED);
            UNIT_ASSERT_C(
                resp->Get()->Record.GetError().GetMessage().Contains("Too many dq operations"),
                TStringBuilder() << "Unexpected error: " << resp->Get()->Record.GetError().GetMessage());
        }
    }

    TActorId RegisterResourceAllocatorOnNode(
        const ui32 workersCount,
        const TActorId& execActor,
        const ui32 nodeIndex) const
    {
        TIntrusivePtr<NMonitoring::TDynamicCounters> counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto gwmActor = MakeWorkerManagerActorID(NodeId());
        TVector<NYql::NDqProto::TDqTask> tasks(workersCount);
        auto allocator = CreateResourceAllocator(
            gwmActor, execActor, execActor, workersCount, "TraceId", new TDqConfiguration(), counters, tasks);
        return ActorRuntime_->Register(allocator, nodeIndex);
    }
};

Y_UNIT_TEST_SUITE(TGlobalWorkerManagerWarmupUploadTest) {
    Y_UNIT_TEST(AnnouncesTheSameLocationsUsedForUpload) {
        TTempDir tempDir;
        const TString localPath = tempDir.Name() + "/udf";
        const TString content = "warmup resource";
        {
            TFileOutput output(localPath);
            output << content;
        }

        TTestActorRuntimeBase runtime(1, true);
        runtime.AddLocalService(GetNameserviceActorId(),
            TActorSetupCmd(NYql::NDqs::CreateDynamicNameserver(MakeNameserverTable(runtime)), TMailboxType::Simple, 0));
        runtime.Initialize();
        const auto nodeId = runtime.GetNodeId(0);
        THashMap<TString, TActorId> wrappers;
        TVector<TResourceManagerOptions> uploadOptions;
        for (const TString cluster : {"one", "two"}) {
            auto& options = uploadOptions.emplace_back();
            options.YtBackend.SetClusterName(cluster);
            options.YtBackend.SetProxyAddress(cluster + ".yt");
            options.YtBackend.SetUser("test-user");
            options.YtBackend.SetPrefix("//locks/" + cluster);
            options.YtBackend.SetUploadPrefix("//unused-backend-prefix");
            options.UploadPrefix = "//gwm/" + cluster;
            wrappers.emplace(options.YtBackend.GetProxyAddress(), runtime.AllocateEdgeActor());
        }
        auto coordinator = MakeIntrusive<TWarmupUploadCoordinator>(nodeId, wrappers);
        const auto gwm = runtime.Register(CreateGlobalWorkerManager(coordinator, uploadOptions,
            CreateMetricsRegistry(MakeIntrusive<TSensorsGroup>()), NProto::TDqConfig::TScheduler()));
        const auto sender = runtime.AllocateEdgeActor();
        const auto attributes = NYT::BuildYsonNodeFluently()
            .BeginMap()
            .Item(NCommonAttrs::ACTOR_NODEID_ATTR).Value(nodeId)
            .Item(NCommonAttrs::HOSTNAME_ATTR).Value("localhost")
            .Item(NCommonAttrs::GRPCPORT_ATTR).Value("1")
            .EndMap();
        runtime.Send(new IEventHandle(gwm, sender, new TEvBecomeLeader(1, "1", NYT::NodeToYsonString(attributes))));
        runtime.Send(new IEventHandle(gwm, sender, new TEvClusterStatus()));
        runtime.GrabEdgeEvent<TEvClusterStatusResponse>(sender);

        Yql::DqsProto::IsReadyRequest request;
        auto* file = request.AddFiles();
        file->SetObjectId("warmup-id");
        file->SetLocalPath(localPath);
        file->SetObjectType(Yql::DqsProto::TFile::EUDF_FILE);
        file->SetSize(content.size());
        runtime.Send(new IEventHandle(gwm, sender, new TEvIsReady(request)));
        runtime.GrabEdgeEvent<TEvIsReadyResponse>(sender);

        ui32 workerId = 100;
        for (const auto& options : uploadOptions) {
            const auto wrapper = wrappers.at(options.YtBackend.GetProxyAddress());
            const auto write = runtime.GrabEdgeEvent<TEvWriteFile>(wrapper, TDuration::Seconds(10));
            UNIT_ASSERT(write);
            const auto remotePath = std::get<1>(*write->Get()).GetPath();
            UNIT_ASSERT_VALUES_EQUAL(remotePath, options.UploadPrefix + "/udfs/warmup-id");
            auto& uploadedFile = std::get<0>(*write->Get());
            TString uploaded = TString::Uninitialized(content.size());
            UNIT_ASSERT_VALUES_EQUAL(uploadedFile.Pread(uploaded.begin(), uploaded.size(), 0), content.size());
            UNIT_ASSERT_VALUES_EQUAL(uploaded, content);
            runtime.Send(new IEventHandle(write->Sender, wrapper,
                new TEvWriteFileResponse(write->Get()->RequestId, NYT::TErrorOr<void>(), TMaybe<bool>())));

            Yql::DqsProto::RegisterNodeRequest worker;
            worker.SetRole("worker_node");
            worker.SetNodeId(++workerId);
            worker.MutableGuid()->SetDw0(workerId);
            worker.SetClusterName(options.YtBackend.GetClusterName());
            worker.AddKnownNodes(nodeId);
            runtime.Send(new IEventHandle(gwm, sender, new TEvRegisterNode(worker)));
            const auto response = runtime.GrabEdgeEvent<TEvRegisterNodeResponse>(sender);
            const auto& groups = response->Get()->Record.GetResponse().GetDownloadGroups();
            AssertWarmupGroups(groups, TWarmupGroups{
                {{options.YtBackend.GetProxyAddress(), options.UploadPrefix}, {"warmup-id"}}});
            UNIT_ASSERT_VALUES_EQUAL(groups.Get(0).GetUploadPrefix() + "/udfs/" + groups.Get(0).GetFiles(0).GetObjectId(), remotePath);
        }
    }
}

UNIT_TEST_SUITE_REGISTRATION(TGlobalWorkerManagerTest)
UNIT_TEST_SUITE_REGISTRATION(TGlobalWorkerManagerWarmupTest)
UNIT_TEST_SUITE_REGISTRATION(TGlobalWorkerManagerRunningLimiterTest)
UNIT_TEST_SUITE_REGISTRATION(TGlobalWorkerManagerRunningLimiterShrinkTest)
UNIT_TEST_SUITE_REGISTRATION(TGlobalWorkerManagerControlRemovalTest)
UNIT_TEST_SUITE_REGISTRATION(TGlobalWorkerManagerStartFollowerTest)
UNIT_TEST_SUITE_REGISTRATION(TGlobalWorkerManagerForwardedExeFileTest)
UNIT_TEST_SUITE_REGISTRATION(TGlobalWorkerManagerQueueProcessingTest)
UNIT_TEST_SUITE_REGISTRATION(TGlobalWorkerManagerQueueLimitsTest)
UNIT_TEST_SUITE_REGISTRATION(TGlobalWorkerManagerDisconnectTest)
