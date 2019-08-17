#include "mock_objects.h"

#include <yp/server/objects/helpers.h>

#include <yp/server/scheduler/helpers.h>

namespace NYP::NServer::NScheduler::NTests {

namespace {

////////////////////////////////////////////////////////////////////////////////

TObjectId GenerateUniqueId()
{
    static int lastObjectIndex = 0;
    return "mock_object_" + ToString(lastObjectIndex++);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TPod> CreateMockPod(ui64 cpuCapacity, ui64 memoryCapacity)
{
    auto uuid = NObjects::GenerateUuid();

    NObjects::TPodResourceRequests resourceRequests;
    resourceRequests.set_vcpu_guarantee(cpuCapacity);
    resourceRequests.set_memory_limit(memoryCapacity);

    return std::make_unique<TPod>(
        GenerateUniqueId(),
        /* labels */ NYT::NYson::TYsonString(),
        /* podSet */ nullptr,
        /* node */ nullptr,
        /* account */ nullptr,
        std::move(uuid),
        std::move(resourceRequests),
        NObjects::TPodDiskVolumeRequests(),
        NObjects::TPodIP6AddressRequests(),
        NObjects::TPodIP6SubnetRequests(),
        /* node filter*/ TString(),
        NClient::NApi::NProto::TPodStatus_TEviction());
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TNode> CreateMockNode(
    THomogeneousResource cpuResource,
    THomogeneousResource memoryResource)
{
    auto node = std::make_unique<TNode>(
        GenerateUniqueId(),
        /* labels */ NYT::NYson::TYsonString(),
        std::vector<TTopologyZone*>(),
        NServer::NObjects::EHfsmState::Unknown,
        NServer::NObjects::ENodeMaintenanceState::None,
        /* hasUnknownPods */ false,
        NClient::NApi::NProto::TNodeSpec());

    node->CpuResource() = cpuResource;
    node->MemoryResource() = memoryResource;

    return node;
}

std::unique_ptr<TNode> CreateMockNode()
{
    return CreateMockNode(
        THomogeneousResource(
            /* total */ MakeCpuCapacities(1000),
            /* allocated */ MakeCpuCapacities(0)),
        THomogeneousResource(
            /* total */ MakeMemoryCapacities(1024 * 1024),
            /* allocated */ MakeMemoryCapacities(0)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler::NTests
