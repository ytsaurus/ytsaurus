#include "serialize.h"

#include <util/generic/cast.h>

namespace NYT::NClusterClock {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

TReign GetCurrentReign()
{
    return ToUnderlying(TEnumTraits<EClockReign>::GetMaxValue());
}

bool ValidateSnapshotReign(TReign reign)
{
    for (auto value : TEnumTraits<EClockReign>::GetDomainValues()) {
        if (ToUnderlying(value) == reign) {
            return true;
        }
    }
    return false;
}

EFinalRecoveryAction GetActionToRecoverFromReign(TReign reign)
{
    // In Clock we do it the hard way.
    YT_VERIFY(reign == GetCurrentReign());

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

EClockReign TSaveContext::GetVersion() const
{
    return static_cast<EClockReign>(NHydra::TSaveContext::GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

TLoadContext::TLoadContext(
    TBootstrap* bootstrap,
    ICheckpointableInputStream* input)
    : NHydra::TLoadContext(input)
    , Bootstrap_(bootstrap)
{ }

EClockReign TLoadContext::GetVersion() const
{
    return static_cast<EClockReign>(NHydra::TLoadContext::GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterClock
