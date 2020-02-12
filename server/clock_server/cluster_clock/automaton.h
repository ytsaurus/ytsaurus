#pragma once

#include "public.h"
#include "serialize.h"

#include <yt/server/lib/hydra/composite_automaton.h>

#include <yt/ytlib/object_client/public.h>
#include <yt/client/object_client/helpers.h>

#include <yt/core/misc/property.h>

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

    virtual std::unique_ptr<NHydra::TSaveContext> CreateSaveContext(
        ICheckpointableOutputStream* output) override;
    virtual std::unique_ptr<NHydra::TLoadContext> CreateLoadContext(
        ICheckpointableInputStream* input) override;

    virtual NHydra::TReign GetCurrentReign() override;
    virtual NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(NHydra::TReign reign) override;
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

    virtual bool ValidateSnapshotVersion(int version) override;
    virtual int GetCurrentSnapshotVersion() override;
};

DEFINE_REFCOUNTED_TYPE(TClockAutomatonPart)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterClock
