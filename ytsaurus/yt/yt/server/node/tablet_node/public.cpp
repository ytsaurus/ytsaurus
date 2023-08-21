#include "public.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

bool IsInUnmountWorkflow(ETabletState state)
{
    return
        state >= ETabletState::UnmountFirst &&
        state <= ETabletState::UnmountLast;
}

bool IsInFreezeWorkflow(ETabletState state)
{
    return
        state >= ETabletState::FreezeFirst &&
        state <= ETabletState::FreezeLast;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
