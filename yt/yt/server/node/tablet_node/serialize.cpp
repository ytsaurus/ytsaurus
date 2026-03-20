#include "serialize.h"

#include <yt/yt/server/lib/tablet_node/private.h>

#include <util/generic/cast.h>

namespace NYT::NTabletNode {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TabletNodeLogger;

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

// This option is set to some large value in tests to test code that triggers
// on reign change.
static TReign ReignOverride = InvalidReign;

TReign GetCurrentReign()
{
    return ReignOverride != InvalidReign
        ? ReignOverride
        : ToUnderlying(TEnumTraits<ETabletReign>::GetMaxValue());
}

bool ValidateSnapshotReign(TReign reign)
{
    if (ReignOverride != InvalidReign && reign == ReignOverride) {
        return true;
    }

    for (auto value : TEnumTraits<ETabletReign>::GetDomainValues()) {
        if (ToUnderlying(value) == reign) {
            return true;
        }
    }
    return false;
}

NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(TReign reign)
{
    YT_LOG_FATAL_UNLESS(reign == GetCurrentReign() || (IsReignChangeAllowed() && reign <= GetCurrentReign()),
        "Attempted to recover tablet cell from invalid reign "
        "(RecoverReign: %v, CurrentReign: %v, IsReignChangeAllowed: %v)",
        reign,
        GetCurrentReign(),
        IsReignChangeAllowed());

    if (reign < GetCurrentReign()) {
        return EFinalRecoveryAction::BuildSnapshotAndRestart;
    }

    return EFinalRecoveryAction::None;
}

////////////////////////////////////////////////////////////////////////////////

namespace NTesting {

////////////////////////////////////////////////////////////////////////////////

void SetCurrentReignOverride(NHydra::TReign reign)
{
    if (ReignOverride != InvalidReign || reign != InvalidReign) {
        YT_LOG_DEBUG("Overriding tablet reign for testing purposes "
            "(PreviousReign: %v, NewReign: %v)",
            GetCurrentReign(),
            reign);

        YT_LOG_FATAL_UNLESS(reign == InvalidReign || reign > ToUnderlying(TEnumTraits<ETabletReign>::GetMaxValue()),
            "Reign override must be either %v or greater than any valid reign",
            InvalidReign);

        ReignOverride = reign;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTesting

////////////////////////////////////////////////////////////////////////////////

TSaveContext::TSaveContext(
    ICheckpointableOutputStream* output,
    NLogging::TLogger logger)
    : NHydra::TSaveContext(
        output,
        std::move(logger),
        GetCurrentReign())
{ }

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
