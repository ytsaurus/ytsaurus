#pragma once

#include "public.h"

#include <yt/yt/server/lib/lsm/lsm_backend.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IStoreCompactor
    : public virtual TRefCounted
{
    // TODO(ifsmirnov): check for semaphores availability.
    virtual void Start() = 0;
    virtual void OnBeginSlotScan() = 0;
    virtual void ProcessLsmActionBatch(
        const ITabletSlotPtr& slot,
        const NLsm::TLsmActionBatch& batch) = 0;
    virtual void OnEndSlotScan() = 0;
    virtual NYTree::IYPathServicePtr GetOrchidService() = 0;
};

DEFINE_REFCOUNTED_TYPE(IStoreCompactor)

IStoreCompactorPtr CreateStoreCompactor(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
