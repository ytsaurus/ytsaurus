#include <contrib/ydb/library/yql/providers/dq/worker_manager/interface/events.h>

#include <yt/yql/providers/dq/worker_manager/interface/allocate_workers_response.h>

#include <yt/yql/providers/dq/common/attrs/attrs.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NDqs {
namespace {

struct TWorkers {
    TSensorsGroupPtr Metrics = MakeIntrusive<TSensorsGroup>();
    TGlobalResources Resources{Metrics};
    TInflightLimiter::TPtr Limiter = MakeIntrusive<TInflightLimiter>(1);

    TWorkerInfo::TPtr Add(ui32 nodeId, const TString& cluster, const THashMap<TString, TString>& attrs) {
        Yql::DqsProto::RegisterNodeRequest request;
        request.SetCapacity(1);
        request.SetClusterName(cluster);
        for (const auto& [key, value] : attrs) {
            auto* attr = request.AddAttribute();
            attr->SetKey(key);
            attr->SetValue(value);
        }
        TGUID workerId;
        CreateGuid(&workerId);
        return MakeIntrusive<TWorkerInfo>("start", nodeId, workerId, request, Metrics, Limiter, Resources);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TAllocateWorkersResponse) {
    Y_UNIT_TEST(EmptyNodesHaveNoError) {
        const auto response = MakeAllocateWorkersResponse(0, {});
        UNIT_ASSERT(response->Record.HasNodes());
        UNIT_ASSERT(!response->Record.HasError());
        UNIT_ASSERT(!response->Record.HasWorkers());
        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetNodes().GetResourceId(), 0);
        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetNodes().WorkerSize(), 0);

        NYql::NDqProto::TAllocateWorkersResponse expected;
        expected.MutableNodes();
        UNIT_ASSERT_VALUES_EQUAL(response->Record.SerializeAsString(), expected.SerializeAsString());
    }

    Y_UNIT_TEST(PreservesWorkerIdentityAttributesAndOrder) {
        TWorkers workers;
        const THashMap<TString, TString> attributes{
            {NCommonAttrs::JOBID_ATTR, "job-one"},
            {NCommonAttrs::OPERATIONID_ATTR, "operation-one"},
            {"custom", "payload"},
        };
        const auto first = workers.Add(101, "alpha", attributes);
        const auto second = workers.Add(202, "beta", {});
        const ui64 resourceId = (ui64{1} << 40) + 17;
        const auto response = MakeAllocateWorkersResponse(resourceId, {first, second});
        UNIT_ASSERT(response->Record.HasNodes());
        UNIT_ASSERT(!response->Record.HasError());
        UNIT_ASSERT(!response->Record.HasWorkers());
        const auto& nodes = response->Record.GetNodes();
        UNIT_ASSERT_VALUES_EQUAL(nodes.GetResourceId(), resourceId);
        UNIT_ASSERT_VALUES_EQUAL(nodes.WorkerSize(), 2);
        const auto& firstNode = nodes.GetWorker(0);
        UNIT_ASSERT_VALUES_EQUAL(firstNode.GetNodeId(), 101);
        UNIT_ASSERT_VALUES_EQUAL(firstNode.GetClusterName(), "alpha");
        UNIT_ASSERT_VALUES_EQUAL(firstNode.GetGuid(), GetGuidAsString(first->WorkerId));
        UNIT_ASSERT_VALUES_EQUAL(firstNode.GetJobId(), "job-one");
        UNIT_ASSERT_VALUES_EQUAL(firstNode.GetOperationId(), "operation-one");
        THashMap<TString, TString> actualAttributes;
        for (const auto& attr : firstNode.GetAttribute()) {
            actualAttributes.emplace(attr.GetKey(), attr.GetValue());
        }
        UNIT_ASSERT_VALUES_EQUAL(firstNode.AttributeSize(), attributes.size());
        UNIT_ASSERT(actualAttributes == attributes);
        const auto& secondNode = nodes.GetWorker(1);
        UNIT_ASSERT_VALUES_EQUAL(secondNode.GetNodeId(), 202);
        UNIT_ASSERT_VALUES_EQUAL(secondNode.GetClusterName(), "beta");
        UNIT_ASSERT_VALUES_EQUAL(secondNode.GetGuid(), GetGuidAsString(second->WorkerId));
        UNIT_ASSERT(secondNode.GetJobId().empty());
        UNIT_ASSERT(secondNode.GetOperationId().empty());
        UNIT_ASSERT_VALUES_EQUAL(secondNode.AttributeSize(), 0);
    }

    Y_UNIT_TEST(SharedConstructorsKeepTheirResponseKinds) {
        const TEvAllocateWorkersResponse error;
        UNIT_ASSERT(error.Record.HasError());
        UNIT_ASSERT_VALUES_EQUAL(error.Record.GetError().GetMessage(), "Unknown error");
        UNIT_ASSERT(!error.Record.HasNodes());

        const TEvAllocateWorkersResponse nodes(31, TVector<ui32>{101, 202});
        UNIT_ASSERT(nodes.Record.HasNodes());
        UNIT_ASSERT(!nodes.Record.HasError());
        UNIT_ASSERT_VALUES_EQUAL(nodes.Record.GetNodes().GetResourceId(), 31);
        UNIT_ASSERT_VALUES_EQUAL(nodes.Record.GetNodes().GetWorker(0).GetNodeId(), 101);
        UNIT_ASSERT_VALUES_EQUAL(nodes.Record.GetNodes().GetWorker(1).GetNodeId(), 202);

        const auto workerId = MakeWorkerManagerActorID(7);
        const TEvAllocateWorkersResponse actors(32, TVector<NActors::TActorId>{workerId});
        UNIT_ASSERT(actors.Record.HasWorkers());
        UNIT_ASSERT(!actors.Record.HasNodes());
        UNIT_ASSERT(!actors.Record.HasError());
        UNIT_ASSERT_VALUES_EQUAL(actors.Record.GetWorkers().GetResourceId(), 32);
        UNIT_ASSERT_VALUES_EQUAL(actors.Record.GetWorkers().WorkerActorSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(NActors::ActorIdFromProto(actors.Record.GetWorkers().GetWorkerActor(0)), workerId);
    }
}

} // namespace NYql::NDqs
