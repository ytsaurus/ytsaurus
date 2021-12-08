#pragma once

#include "public.h"
#include "serialize.h"

#include <yt/yt/server/lib/hydra_common/composite_automaton.h>

#include <yt/yt/ytlib/object_client/public.h>
#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/property.h>

namespace NYT::NClusterClock {

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NHydra::TSaveContext
{
public:
    EClockReign GetVersion();
};

////////////////////////////////////////////////////////////////////////////////

class TLoadContext
    : public NHydra::TLoadContext
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TBootstrap*, Bootstrap);

public:
    explicit TLoadContext(TBootstrap* bootstrap);

    EClockReign GetVersion();
};

////////////////////////////////////////////////////////////////////////////////

class TClockAutomaton
    : public NHydra::TCompositeAutomaton
{
public:
    explicit TClockAutomaton(TBootstrap* boostrap);

private:
    TBootstrap* const Bootstrap_;

    std::unique_ptr<NHydra::TSaveContext> CreateSaveContext(
        ICheckpointableOutputStream* output) override;
    std::unique_ptr<NHydra::TLoadContext> CreateLoadContext(
        ICheckpointableInputStream* input) override;

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
