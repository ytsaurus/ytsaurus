#include "serialize.h"

#include <yt/yt/server/lib/tablet_node/private.h>

#include <util/generic/cast.h>

namespace NYT::NTabletNode {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletNodeLogger;

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

TSaveContext::TSaveContext(
    ICheckpointableOutputStream* output,
    NLogging::TLogger logger)
    : NLeaseServer::TSaveContext(
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
