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
    NServer::NObjects::NProto::TMetaEtc metaEtc;
    metaEtc.set_uuid(NYP::NServer::NObjects::GenerateUuid());

    NServer::NObjects::NProto::TPodSpecEtc specEtc;
    specEtc.mutable_resource_requests()->set_vcpu_guarantee(cpuCapacity);
    specEtc.mutable_resource_requests()->set_memory_limit(memoryCapacity);

    return std::make_unique<TPod>(
        GenerateUniqueId(),
        /* podSet */ nullptr,
        std::move(metaEtc),
        /* node */ nullptr,
        specEtc,
        /* node */ nullptr,
        NObjects::NProto::TPodStatusEtc(),
        /* labels */ NYT::NYson::TYsonString());
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
