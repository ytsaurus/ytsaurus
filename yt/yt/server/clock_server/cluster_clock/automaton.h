#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/composite_automaton.h>

#include <yt/yt/ytlib/object_client/public.h>

namespace NYT::NClusterClock {

////////////////////////////////////////////////////////////////////////////////

class TClockAutomaton
    : public NHydra::TCompositeAutomaton
{
public:
    explicit TClockAutomaton(TBootstrap* bootstrap);

private:
    TBootstrap* const Bootstrap_;

    std::unique_ptr<NHydra::TSaveContext> CreateSaveContext(
        NHydra::ICheckpointableOutputStream* output,
        NLogging::TLogger logger) override;
    std::unique_ptr<NHydra::TLoadContext> CreateLoadContext(
        NHydra::ICheckpointableInputStream* input) override;

    NHydra::TReign GetCurrentReign() override;
    NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(NHydra::TReign reign) override;
};

DEFINE_REFCOUNTED_TYPE(TClockAutomaton)

////////////////////////////////////////////////////////////////////////////////

class TClockAutomatonPart
    : public NHydra::TCompositeAutomatonPart
{
protected:
    TBootstrap* const Bootstrap_;

    TClockAutomatonPart(
        TBootstrap* bootstrap,
        EAutomatonThreadQueue queue);

    bool ValidateSnapshotVersion(int version) override;
    int GetCurrentSnapshotVersion() override;
};

DEFINE_REFCOUNTED_TYPE(TClockAutomatonPart)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterClock
