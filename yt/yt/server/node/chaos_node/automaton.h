#pragma once

#include "public.h"
#include "serialize.h"

#include <yt/yt/server/lib/hydra/composite_automaton.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/logging/logger_owner.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

//! An instance of Hydra automaton managing chaos cells.
class TChaosAutomaton
    : public NHydra::TCompositeAutomaton
{
public:
    explicit TChaosAutomaton(IChaosAutomatonHostPtr host);

private:
    const TWeakPtr<IChaosAutomatonHost> Host_;

    std::unique_ptr<NHydra::TSaveContext> CreateSaveContext(
        NHydra::ICheckpointableOutputStream* output,
        NLogging::TLogger logger) override;
    std::unique_ptr<NHydra::TLoadContext> CreateLoadContext(
        NHydra::ICheckpointableInputStream* input) override;

    NHydra::TReign GetCurrentReign() override;
    NHydra::TReign GetCurrentTractoReign() override;
    NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(NHydra::TReign reign) override;
    NHydra::EFinalRecoveryAction GetActionToRecoverFromTractoReign(NHydra::TReign reign) override;
};

DEFINE_REFCOUNTED_TYPE(TChaosAutomaton)

////////////////////////////////////////////////////////////////////////////////

class TChaosAutomatonPart
    : public NHydra::TCompositeAutomatonPart
    , public virtual NLogging::TLoggerOwner
{
protected:
    const IChaosSlotPtr Slot_;
    IBootstrap* const Bootstrap_;

    TChaosAutomatonPart(
        IChaosSlotPtr slot,
        IBootstrap* bootstrap);

    bool ValidateSnapshotVersion(int version) override;
    int GetCurrentSnapshotVersion() override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
