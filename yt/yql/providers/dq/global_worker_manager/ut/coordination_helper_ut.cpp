#include <yt/yql/providers/dq/global_worker_manager/coordination_helper.h>

#include <contrib/ydb/library/actors/testlib/test_runtime.h>

#include <contrib/ydb/library/yql/providers/dq/actors/events/events.h>

#include <contrib/ydb/library/yql/providers/dq/common/attrs.h>

#include <library/cpp/testing/unittest/registar.h>

#include <library/cpp/yson/node/node_io.h>

using namespace NActors;
using namespace NYql;

Y_UNIT_TEST_SUITE(TCoordinationHelperTest) {
    Y_UNIT_TEST(LockUsesHostnameByDefaultAndIpWhenRequested) {
        for (const auto& [ip, grpcHost] : TVector<std::pair<TString, TString>>{
                 {"2001:db8::1", "[2001:db8::1]"},
                 {"192.0.2.1", "192.0.2.1"},
             }) {
            for (bool useIp : {false, true}) {
                TTestActorRuntimeBase runtime;
                runtime.Initialize();
                const auto parent = runtime.AllocateEdgeActor();

                NProto::TDqConfig::TYtCoordinator config;
                config.SetLockType("dummy");
                const TString hostname = "qtworker.example.test";
                const auto coordinator = useIp
                    ? CreateCoordiantionHelper(config, /*schedulerConfig*/ {}, "service_node", /*interconnectPort*/ 31002, hostname, ip, /*actorIdOptions*/ {}, EGwmHostAddress::Ip)
                    : CreateCoordiantionHelper(config, /*schedulerConfig*/ {}, "service_node", /*interconnectPort*/ 31002, hostname, ip);
                coordinator->GetNodeId(runtime.GetNodeId(), TString("31003"), /*minNodeId*/ 0, /*maxNodeId*/ 0, /*attributes*/ {});

                runtime.Register(coordinator->CreateLock("gwm"), 0, 0, TMailboxType::Simple, 0, parent);
                const auto leader = runtime.GrabEdgeEvent<TEvBecomeLeader>(parent, TDuration::Seconds(5));
                const auto attributes = NYT::NodeFromYsonString(leader->Get()->Attributes);
                UNIT_ASSERT_VALUES_EQUAL(
                    attributes[NCommonAttrs::HOSTNAME_ATTR].AsString(),
                    useIp ? grpcHost : hostname);
            }
        }
    }
}
