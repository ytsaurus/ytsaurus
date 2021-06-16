#pragma once

#include "public.h"

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/core/concurrency/spinlock.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

class TTabletSyncReplicaCache
    : public TRefCounted    
{
public:
    TTabletSyncReplicaCache();

    std::vector<TTableReplicaIdList> Filter(
        THashMap<NObjectClient::TCellId, std::vector<NTabletClient::TTabletId>>* cellIdToTabletIds,
        TInstant cachedSyncReplicasDeadline);

    void Put(
        TInstant cachedSyncReplicasAt,
        THashMap<NTabletClient::TTabletId, TTableReplicaIdList> tabletIdToSyncReplicaIds);

private:
    struct TTabletSyncReplicaIds
    {
        YT_DECLARE_SPINLOCK(TAdaptiveLock, Lock);

        TInstant CachedSyncReplicasAt;
        TTableReplicaIdList SyncReplicaIds;
    };

    YT_DECLARE_SPINLOCK(NConcurrency::TReaderWriterSpinLock, Lock);
    THashMap<NTabletClient::TTabletId, TTabletSyncReplicaIds> TabletIdToSyncReplicaIds_;


    void ScheduleSweepObsoleteEntires();
    void DoSweepObsoleteEntires();
};

DEFINE_REFCOUNTED_TYPE(TTabletSyncReplicaCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
