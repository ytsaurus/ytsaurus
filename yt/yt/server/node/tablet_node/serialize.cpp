#include "serialize.h"

#include <util/generic/cast.h>

namespace NYT::NTabletNode {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

TReign GetCurrentReign()
{
    return ToUnderlying(TEnumTraits<ETabletReign>::GetMaxValue());
}

bool ValidateSnapshotReign(TReign reign)
{
    for (auto v : TEnumTraits<ETabletReign>::GetDomainValues()) {
        if (ToUnderlying(v) == reign) {
            return true;
        }
    }
    return false;
}

NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(TReign reign)
{
    YT_VERIFY(reign <= GetCurrentReign());

    if (reign < GetCurrentReign()) {
        return EFinalRecoveryAction::BuildSnapshotAndRestart;
    }

    return EFinalRecoveryAction::None;
}

////////////////////////////////////////////////////////////////////////////////

ETabletReign TSaveContext::GetVersion() const
{
    return static_cast<ETabletReign>(NHydra::TSaveContext::GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

ETabletReign TLoadContext::GetVersion() const
{
    return static_cast<ETabletReign>(NHydra::TLoadContext::GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
