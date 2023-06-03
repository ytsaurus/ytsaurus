#pragma once

#include "public.h"

#include <yt/yt/client/tablet_client/public.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>
#include <library/cpp/yt/threading/spin_lock.h>

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
        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock);

        TInstant CachedSyncReplicasAt;
        TTableReplicaIdList SyncReplicaIds;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock);
    THashMap<NTabletClient::TTabletId, TTabletSyncReplicaIds> TabletIdToSyncReplicaIds_;


    void ScheduleSweepObsoleteEntries();
    void DoSweepObsoleteEntries();
};

DEFINE_REFCOUNTED_TYPE(TTabletSyncReplicaCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
