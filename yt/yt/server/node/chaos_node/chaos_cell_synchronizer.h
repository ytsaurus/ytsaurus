#pragma once

#include "public.h"

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

struct IChaosCellSynchronizer
    : public TRefCounted
{
    virtual void Start() = 0;
    virtual void Stop() = 0;
};

DEFINE_REFCOUNTED_TYPE(IChaosCellSynchronizer)

IChaosCellSynchronizerPtr CreateChaosCellSynchronizer(
    TChaosCellSynchronizerConfigPtr config,
    IChaosSlotPtr slot,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
