#include "persistent_state.h"

#include <yt/yt/server/scheduler/strategy/policy/persistent_state.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TPersistentOperationState::Register(TRegistrar registrar)
{
    registrar.Parameter("scheduling_module", &TThis::SchedulingModule)
        .Default()
        .DontSerializeDefault();
    registrar.Parameter("network_priority", &TThis::NetworkPriority)
        .Default()
        .DontSerializeDefault();
}

////////////////////////////////////////////////////////////////////////////////

void TPersistentNodeState::Register(TRegistrar registrar)
{
    registrar.Parameter("scheduling_module", &TThis::SchedulingModule)
        .Default()
        .DontSerializeDefault();
    registrar.Parameter("address", &TThis::Address);
}

////////////////////////////////////////////////////////////////////////////////

void TPersistentState::Register(TRegistrar registrar)
{
    registrar.Parameter("node_states", &TPersistentState::NodeStates)
        .Default();
    registrar.Parameter("operation_states", &TPersistentState::OperationStates)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

INodePtr ConvertClassicToGpuPersistentState(const INodePtr& node)
{
    auto classicState = ConvertTo<NPolicy::TPersistentStatePtr>(node);
    const auto& classicOperationStates = classicState->SchedulingSegmentsState->OperationStates;

    auto gpuState = New<TPersistentState>();
    gpuState->OperationStates.reserve(classicOperationStates.size());
    for (const auto& [operationId, operationState] : classicOperationStates) {
        if (operationState.Module) {
            TPersistentOperationState gpuOperationState;
            gpuOperationState.SchedulingModule = operationState.Module;
            EmplaceOrCrash(gpuState->OperationStates, operationId, std::move(gpuOperationState));
        }
    }

    return ConvertToNode(gpuState);
}

////////////////////////////////////////////////////////////////////////////////

INodePtr ConvertGpuToClassicPersistentState(const INodePtr& node)
{
    auto gpuState = ConvertTo<TPersistentStatePtr>(node);

    auto classicState = New<NPolicy::TPersistentState>();
    auto& classicOperationStates = classicState->SchedulingSegmentsState->OperationStates;
    classicOperationStates.reserve(gpuState->OperationStates.size());
    for (const auto& [operationId, operationState] : gpuState->OperationStates) {
        if (operationState.SchedulingModule) {
            EmplaceOrCrash(
                classicOperationStates,
                operationId,
                NPolicy::TPersistentOperationSchedulingSegmentState{.Module = operationState.SchedulingModule});
        }
    }

    return ConvertToNode(classicState);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
