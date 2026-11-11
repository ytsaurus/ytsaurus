#include <yt/yql/providers/dq/global_worker_manager/global_worker_manager.h>
#include <yt/yql/providers/dq/testlib/test_coordination_helper.h>

#include <contrib/ydb/library/actors/interconnect/interconnect.h>
#include <contrib/ydb/library/actors/testlib/test_runtime.h>
#include <contrib/ydb/library/yql/providers/dq/actors/events/events.h>
#include <contrib/ydb/library/yql/providers/dq/actors/execution_helpers.h>
#include <contrib/ydb/library/yql/providers/dq/common/attrs.h>

#include <library/cpp/svnversion/svnversion.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/yson/node/node_io.h>

#include <yql/essentials/utils/log/log.h>

using namespace NActors;
using namespace NYql;
using namespace NYql::NDqs;

namespace {

TString LeaderAttributes(ui32 nodeId) {
    auto attributes = NYT::TNode::CreateMap();
    attributes[NCommonAttrs::ACTOR_NODEID_ATTR] = nodeId;
    attributes[NCommonAttrs::HOSTNAME_ATTR] = "localhost";
    attributes[NCommonAttrs::GRPCPORT_ATTR] = "1234";
    attributes[NCommonAttrs::REVISION_ATTR] = GetProgramCommitId();
    return NYT::NodeToYsonString(attributes);
}

struct TWorkerManagerTest {
    TWorkerManagerTest() {
        NYql::NLog::YqlLogger().SetComponentLevel(NYql::NLog::EComponent::ProviderDq, NYql::NLog::ELevel::TRACE);
        CreateGuid(&WorkerId);
        auto nodes = MakeIntrusive<TTableNameserverSetup>();
        for (ui32 node = 0; node < 2; ++node) {
            nodes->StaticNodeTable[Runtime.GetNodeId(node)] = TTableNameserverSetup::TNodeInfo("::1", "localhost", 1234 + node);
        }
        for (ui32 node = 0; node < 2; ++node) {
            Runtime.AddLocalService(GetNameserviceActorId(),
                TActorSetupCmd(CreateNameserverTable(nodes), TMailboxType::ReadAsFilled, 0), node);
        }
        Runtime.Initialize();
        AllocatorProxy = Runtime.GetInterconnectProxy(0, 1);
        Edge = Runtime.AllocateEdgeActor();
        Allocator = Runtime.AllocateEdgeActor(1);
        Runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvRegisterNode::EventType &&
                event->Recipient == GetNameserviceActorId()) {
                // Registration has updated worker storage; the nameservice is outside this test.
                return TTestActorRuntimeBase::EEventAction::DROP;
            }
            if (event->GetTypeRewrite() == TEvents::TEvUnsubscribe::EventType &&
                event->Sender == Gwm && event->GetRecipientRewrite() != Edge) {
                // Capture the outgoing cleanup request without depending on IC dispatch order.
                event->Rewrite(TEvents::TEvUnsubscribe::EventType, Edge);
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });
        Gwm = Runtime.Register(CreateGlobalWorkerManager(
            MakeIntrusive<TTestCoordinationHelper>(Runtime.GetNodeId()),
            {},
            CreateMetricsRegistry(MakeIntrusive<TSensorsGroup>()),
            NProto::TDqConfig::TScheduler()));
        TDispatchOptions bootstrap;
        bootstrap.FinalEvents.emplace_back(TEvents::TEvBootstrap::EventType, 1);
        Runtime.DispatchEvents(bootstrap);
        BecomeLeader();
    }

    ~TWorkerManagerTest() {
        Runtime.SetObserverFunc(TTestActorRuntimeBase::DefaultObserverFunc);
    }

    void CheckResponsive() {
        Runtime.Send(new IEventHandle(Gwm, Edge, new TEvGetMasterRequest()));
        auto response = Runtime.GrabEdgeEvent<TEvGetMasterResponse>(Edge, TDuration::Seconds(5));
        UNIT_ASSERT(response);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetResponse().GetPort(), 1234);
    }

    void BecomeLeader() {
        Runtime.Send(new IEventHandle(Gwm, Edge,
            new TEvBecomeLeader(++Epoch, "transaction", LeaderAttributes(Runtime.GetNodeId()))));
        CheckResponsive();
    }

    void BecomeFollower() {
        Runtime.Send(new IEventHandle(Gwm, Edge,
            new TEvBecomeFollower(LeaderAttributes(Allocator.NodeId()))));
        CheckResponsive();
    }

    void ExpectUnsubscribe() {
        auto event = Runtime.GrabEdgeEvent<TEvents::TEvUnsubscribe>(Edge, TDuration::Seconds(5));
        UNIT_ASSERT(event);
        UNIT_ASSERT_VALUES_EQUAL(event->Recipient, AllocatorProxy);
        UNIT_ASSERT_VALUES_EQUAL(event->Sender, Gwm);
    }

    void AllocateWorker() {
        auto registration = MakeHolder<TEvRegisterNode>();
        auto* request = registration->Record.MutableRequest();
        request->SetNodeId(Allocator.NodeId());
        request->SetRole("worker_node");
        request->SetClusterName("test");
        request->SetRevision(GetProgramCommitId());
        request->SetCapacity(1);
        request->AddKnownNodes(Runtime.GetNodeId());
        NExecutionHelpers::GuidToProto(*request->MutableGuid(), WorkerId);
        Runtime.Send(new IEventHandle(Gwm, Edge, registration.Release()));
        CheckResponsive();

        Runtime.Send(new IEventHandle(Gwm, Allocator, new TEvAllocateWorkersRequest(1, "user")));
        Runtime.Send(new IEventHandle(Gwm, Edge, new TEvTick()));
        auto allocation = Runtime.GrabEdgeEvent<TEvAllocateWorkersResponse>(Allocator, TDuration::Seconds(5));
        UNIT_ASSERT(allocation);
        UNIT_ASSERT_VALUES_EQUAL(allocation->Get()->Record.GetNodes().WorkerSize(), 1);
        CheckResponsive();
    }

    NYql::NLog::YqlLoggerScope Log{"cerr", false};
    TTestActorRuntimeBase Runtime{2};
    TActorId Edge;
    TActorId Allocator;
    TActorId AllocatorProxy;
    TActorId Gwm;
    TGUID WorkerId;
    ui32 Epoch = 0;
};

} // namespace

Y_UNIT_TEST_SUITE(TGlobalWorkerManagerInterconnectTest) {
    Y_UNIT_TEST(DemotionUnsubscribesAllocatorSessions) {
        TWorkerManagerTest env;
        env.AllocateWorker();
        env.BecomeFollower();
        env.ExpectUnsubscribe();
    }

    Y_UNIT_TEST(LateInterconnectEventsAfterDemotion) {
        TWorkerManagerTest env;
        env.AllocateWorker();
        env.BecomeFollower();
        env.ExpectUnsubscribe();

        // These notifications may have been enqueued before the unsubscribe took effect.
        env.Runtime.Send(new IEventHandle(env.Gwm, env.Allocator,
            new TEvInterconnect::TEvNodeDisconnected(env.Allocator.NodeId())));
        env.Runtime.Send(new IEventHandle(env.Gwm, env.Allocator,
            new TEvInterconnect::TEvNodeConnected(env.Allocator.NodeId())));
        env.Runtime.Send(new IEventHandle(env.Gwm, env.Allocator,
            new TEvents::TEvUndelivered(TEvAllocateWorkersResponse::EventType, TEvents::TEvUndelivered::Disconnected)));
        env.ExpectUnsubscribe();
        env.CheckResponsive();

        env.BecomeLeader();
        env.AllocateWorker();
    }
}
