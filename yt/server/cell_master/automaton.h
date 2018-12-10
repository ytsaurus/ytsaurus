#pragma once

#include "public.h"

#include <yt/server/chunk_server/public.h>

#include <yt/server/hydra/composite_automaton.h>

#include <yt/server/table_server/public.h>

#include <yt/server/object_server/public.h>

#include <yt/ytlib/object_client/public.h>
#include <yt/client/object_client/helpers.h>

#include <yt/core/misc/property.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NHydra::TSaveContext
{
public:
    using TSavedSchemaMap = THashMap<NTableServer::TSharedTableSchema*, NObjectClient::TVersionedObjectId>;
    DEFINE_BYREF_RW_PROPERTY(TSavedSchemaMap, SavedSchemas);
};

////////////////////////////////////////////////////////////////////////////////

class TLoadContext
    : public NHydra::TLoadContext
{
public:
    using TLoadedSchemaMap = THashMap<
        NObjectClient::TVersionedObjectId,
        NTableServer::TSharedTableSchema*,
        NObjectClient::TDirectVersionedObjectIdHash>;
    DEFINE_BYVAL_RO_PROPERTY(TBootstrap*, Bootstrap);
    DEFINE_BYREF_RW_PROPERTY(TLoadedSchemaMap, LoadedSchemas);

public:
    explicit TLoadContext(TBootstrap* bootstrap);

    NObjectServer::TObjectBase* GetWeakGhostObject(const NObjectServer::TObjectId& id) const;
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

} // namespace NYT::NCellMaster
