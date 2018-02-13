#pragma once

#include "public.h"

#include <yt/server/chunk_server/public.h>

#include <yt/server/hydra/composite_automaton.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/core/misc/property.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NHydra::TSaveContext
{ };

////////////////////////////////////////////////////////////////////////////////

class TLoadContext
    : public NHydra::TLoadContext
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TBootstrap*, Bootstrap);

public:
    explicit TLoadContext(TBootstrap* bootstrap);

    template <class T>
    T* Get(const NObjectClient::TObjectId& id) const;

    template <class T>
    T* Get(const NObjectClient::TVersionedObjectId& id) const;

    template <class T>
    T* Get(NChunkServer::TNodeId id) const;

};

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

} // namespace NCellMaster
} // namespace NYT
