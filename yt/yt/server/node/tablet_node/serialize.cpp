#include "serialize.h"

#include <util/generic/cast.h>

namespace NYT::NTabletNode {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static bool ReignChangeAllowed = true;

void SetReignChangeAllowed(bool allowed)
{
    ReignChangeAllowed = allowed;
}

bool IsReignChangeAllowed()
{
    return ReignChangeAllowed;
}

////////////////////////////////////////////////////////////////////////////////

TReign GetCurrentReign()
{
    return ToUnderlying(TEnumTraits<ETabletReign>::GetMaxValue());
}

bool ValidateSnapshotReign(TReign reign)
{
    for (auto value : TEnumTraits<ETabletReign>::GetDomainValues()) {
        if (ToUnderlying(value) == reign) {
            return true;
        }
    }
    return false;
}

NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(TReign reign)
{
    YT_VERIFY(reign <= GetCurrentReign());

    if (!IsReignChangeAllowed()) {
        YT_VERIFY(reign == GetCurrentReign());
    }

    if (reign < GetCurrentReign()) {
        return EFinalRecoveryAction::BuildSnapshotAndRestart;
    }

    return EFinalRecoveryAction::None;
}

////////////////////////////////////////////////////////////////////////////////

TSaveContext::TSaveContext(ICheckpointableOutputStream* output)
    : NHydra::TSaveContext(output, GetCurrentReign())
{ }

ETabletReign TSaveContext::GetVersion() const
{
    return static_cast<ETabletReign>(NHydra::TSaveContext::GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

TLoadContext::TLoadContext(ICheckpointableInputStream* input)
    : NHydra::TLoadContext(input)
{ }

ETabletReign TLoadContext::GetVersion() const
{
    return static_cast<ETabletReign>(NHydra::TLoadContext::GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
