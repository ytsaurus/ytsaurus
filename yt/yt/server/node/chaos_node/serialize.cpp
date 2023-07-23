#include "serialize.h"

#include <util/generic/cast.h>

namespace NYT::NChaosNode {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

TReign GetCurrentReign()
{
    return ToUnderlying(TEnumTraits<EChaosReign>::GetMaxValue());
}

bool ValidateSnapshotReign(TReign reign)
{
    for (auto v : TEnumTraits<EChaosReign>::GetDomainValues()) {
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

TSaveContext::TSaveContext(
    ICheckpointableOutputStream* output,
    NLogging::TLogger logger)
    : NHydra::TSaveContext(
        output,
        std::move(logger),
        GetCurrentReign())
{ }

EChaosReign TSaveContext::GetVersion() const
{
    return static_cast<EChaosReign>(NHydra::TSaveContext::GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

TLoadContext::TLoadContext(ICheckpointableInputStream* input)
    : NHydra::TLoadContext(input)
{ }

EChaosReign TLoadContext::GetVersion() const
{
    return static_cast<EChaosReign>(NHydra::TLoadContext::GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
