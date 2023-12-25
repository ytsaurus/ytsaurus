#include "serialize.h"

#include "private.h"

#include <util/generic/cast.h>

namespace NYT::NChaosNode {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChaosNodeLogger;

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
    YT_LOG_FATAL_UNLESS(reign <= GetCurrentReign(),
        "Attempted to recover chaos cell from invalid reign "
        "(RecoverReign: %v, CurrentReign: %v)",
        reign,
        GetCurrentReign());

    if (reign < GetCurrentReign()) {
        return EFinalRecoveryAction::BuildSnapshotAndRestart;
    }

    return EFinalRecoveryAction::None;
}

////////////////////////////////////////////////////////////////////////////////

TSaveContext::TSaveContext(
    ICheckpointableOutputStream* output,
    NLogging::TLogger logger)
    : NYT::NLeaseServer::TSaveContext(
        output,
        std::move(logger),
        GetCurrentReign())
{ }

EChaosReign TSaveContext::GetVersion() const
{
    return static_cast<EChaosReign>(NHydra::TSaveContext::GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

EChaosReign TLoadContext::GetVersion() const
{
    return static_cast<EChaosReign>(NHydra::TLoadContext::GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
