#include "operation.h"

namespace NYT::NScheduler::NStrategy {

////////////////////////////////////////////////////////////////////////////////

void TOperationPoolTreeAttributes::Register(TRegistrar registrar)
{
    registrar.Parameter("slot_index", &TThis::SlotIndex)
        .Default();

    registrar.Parameter("running_in_ephemeral_pool", &TThis::RunningInEphemeralPool)
        .Default();

    registrar.Parameter("running_in_lightweight_pool", &TThis::RunningInLightweightPool)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy
