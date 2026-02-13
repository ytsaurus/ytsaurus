#include "persistent_state.h"

namespace NYT::NScheduler::NStrategy::NPolicy::NGpu {

////////////////////////////////////////////////////////////////////////////////

void TPersistentOperationState::Register(TRegistrar registrar)
{
    registrar.Parameter("scheduling_module", &TThis::SchedulingModule)
        .Default()
        .DontSerializeDefault();
}

////////////////////////////////////////////////////////////////////////////////

void TPersistentAssignmentState::Register(TRegistrar registrar)
{
    registrar.Parameter("node_id", &TThis::NodeId);
    registrar.Parameter("operation_id", &TThis::OperationId);
    registrar.Parameter("allocation_group_name", &TThis::AllocationGroupName);
    registrar.Parameter("resource_usage", &TThis::ResourceUsage);
    registrar.Parameter("creation_time", &TThis::CreationTime);
    registrar.Parameter("preemptible", &TThis::Preemptible);
}

////////////////////////////////////////////////////////////////////////////////

void TPersistentNodeState::Register(TRegistrar registrar)
{
    registrar.Parameter("assignment_states", &TThis::AssignmentStates)
        .Default();
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

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
