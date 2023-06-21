#pragma once

#include "public.h"

#include <yt/yt/client/hydra/public.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

struct IForeignMigratedReplicationCardRemover
    : public TRefCounted
{
    virtual void Start() = 0;
    virtual void Stop() = 0;
};

DEFINE_REFCOUNTED_TYPE(IForeignMigratedReplicationCardRemover)

IForeignMigratedReplicationCardRemoverPtr CreateForeignMigratedReplicationCardRemover(
    TForeignMigratedReplicationCardRemoverConfigPtr config,
    IChaosSlotPtr slot,
    NHydra::ISimpleHydraManagerPtr hydraManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
