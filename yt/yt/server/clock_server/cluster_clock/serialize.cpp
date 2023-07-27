#include "serialize.h"

#include "private.h"

#include <util/generic/cast.h>

namespace NYT::NClusterClock {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ClusterClockLogger;

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
    YT_LOG_FATAL_UNLESS(reign == GetCurrentReign(),
        "Attempted to recover clock from invalid reign "
        "(RecoverReign: %v, CurrentReign: %v)",
        reign,
        GetCurrentReign());

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
