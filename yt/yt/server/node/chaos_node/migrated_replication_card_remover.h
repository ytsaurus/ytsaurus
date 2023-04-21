#pragma once

#include "public.h"

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

struct IMigratedReplicationCardRemover
    : public TRefCounted
{
    virtual void Start() = 0;
    virtual void Stop() = 0;

    virtual void Save(TSaveContext& context) const = 0;
    virtual void Load(TLoadContext& context) = 0;
    virtual void Clear() = 0;

    virtual void EnqueueRemoval(TReplicationCardId migratedReplicationCardId) = 0;
    virtual void ConfirmRemoval(TReplicationCardId migratedReplicationCardId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMigratedReplicationCardRemover)

IMigratedReplicationCardRemoverPtr CreateMigratedReplicationCardRemover(
    TMigratedReplicationCardRemoverConfigPtr config,
    IChaosSlotPtr slot,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
