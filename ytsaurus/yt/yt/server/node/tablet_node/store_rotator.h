#pragma once

#include "public.h"

#include <yt/yt/server/lib/lsm/lsm_backend.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IStoreRotator
    : public virtual TRefCounted
{
    virtual void ProcessLsmActionBatch(
        const ITabletSlotPtr& slot,
        const NLsm::TLsmActionBatch& batch) = 0;
};

DEFINE_REFCOUNTED_TYPE(IStoreRotator)

IStoreRotatorPtr CreateStoreRotator(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
