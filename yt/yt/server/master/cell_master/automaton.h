#pragma once

#include "public.h"

#include <yt/server/master/chunk_server/public.h>

#include <yt/server/lib/hydra/composite_automaton.h>

#include <yt/server/master/table_server/public.h>

#include <yt/server/master/object_server/public.h>

#include <yt/server/master/security_server/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/misc/property.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TMasterAutomaton
    : public NHydra::TCompositeAutomaton
{
public:
    explicit TMasterAutomaton(TBootstrap* boostrap);

private:
    TBootstrap* const Bootstrap_;


    virtual std::unique_ptr<NHydra::TSaveContext> CreateSaveContext(
        ICheckpointableOutputStream* output) override;
    virtual std::unique_ptr<NHydra::TLoadContext> CreateLoadContext(
        ICheckpointableInputStream* input) override;

    virtual NHydra::TReign GetCurrentReign() override;
    virtual NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(NHydra::TReign reign) override;
};

DEFINE_REFCOUNTED_TYPE(TMasterAutomaton)

////////////////////////////////////////////////////////////////////////////////

class TMasterAutomatonPart
    : public NHydra::TCompositeAutomatonPart
{
protected:
    TBootstrap* const Bootstrap_;


    TMasterAutomatonPart(
        TBootstrap* bootstrap,
        EAutomatonThreadQueue queue);

    virtual bool ValidateSnapshotVersion(int version) override;
    virtual int GetCurrentSnapshotVersion() override;
};

DEFINE_REFCOUNTED_TYPE(TMasterAutomatonPart)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
