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

} // namespace NYT::NScheduler::NStrategy::NPolicy::NGpu
