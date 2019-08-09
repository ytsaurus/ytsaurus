#include "serialize.h"

#include <util/generic/cast.h>

namespace NYT::NCellMaster {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

TReign GetCurrentReign()
{
    return ToUnderlying(TEnumTraits<EMasterReign>::GetMaxValue());
}

bool ValidateSnapshotReign(TReign reign)
{
    for (auto v : TEnumTraits<EMasterReign>::GetDomainValues()) {
        if (v == reign) {
            return true;
        }
    }
    return false;
}

EFinalRecoveryAction GetActionToRecoverFromReign(TReign reign)
{
    // In Master we do it the hard way.
    YT_VERIFY(reign == GetCurrentReign());

    return EFinalRecoveryAction::None;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
