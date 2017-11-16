#include "public.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

const TString UnknownProfilingTag("unknown");

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

} // namespace NTabletNode
} // namespace NYT
