#pragma once

#include "public.h"
#include "background_activity_orchid.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TFlushTaskInfo
    : public TBackgroundActivityTaskInfoBase
{
    const TStoreId StoreId;

    TRuntimeData RuntimeData;

    TFlushTaskInfo(
        TGuid taskId,
        TTabletId tabletId,
        NHydra::TRevision mountRevision,
        NYPath::TYPath tablePath,
        std::string tabletCellBundle,
        TStoreId storeId);

    bool ComparePendingTasks(const TFlushTaskInfo& other) const;
};

DEFINE_REFCOUNTED_TYPE(TFlushTaskInfo);

////////////////////////////////////////////////////////////////////////////////

struct IStoreFlusher
    : public virtual TRefCounted
{
    virtual void Start() = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IStoreFlusher)

IStoreFlusherPtr CreateStoreFlusher(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
