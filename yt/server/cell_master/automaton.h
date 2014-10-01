#pragma once

#include "public.h"

#include <core/misc/property.h>

#include <ytlib/object_client/public.h>

#include <server/hydra/composite_automaton.h>

#include <server/chunk_server/public.h>

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

    virtual TSaveContext& SaveContext() override;
    virtual TLoadContext& LoadContext() override;

private:
    std::unique_ptr<TSaveContext> SaveContext_;
    std::unique_ptr<TLoadContext> LoadContext_;

};

DEFINE_REFCOUNTED_TYPE(TMasterAutomaton)

////////////////////////////////////////////////////////////////////////////////

class TMasterAutomatonPart
    : public NHydra::TCompositeAutomatonPart
{
protected:
    TBootstrap* Bootstrap;

    explicit TMasterAutomatonPart(TBootstrap* bootstrap);

    virtual bool ValidateSnapshotVersion(int version) override;
    virtual int GetCurrentSnapshotVersion() override;

    void RegisterSaver(
        int priority,
        const Stroka& name,
        TCallback<void(TSaveContext&)> saver);

    void RegisterLoader(
        const Stroka& name,
        TCallback<void(TLoadContext&)> loader);

};

DEFINE_REFCOUNTED_TYPE(TMasterAutomatonPart)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
