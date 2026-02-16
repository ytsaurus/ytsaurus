#pragma once

#include "public.h"
#include "store.h"
#include "partition.h"
#include "statistics.h"

#include <yt/yt/core/misc/public.h>

#include <yt/yt/client/table_client/key.h>

namespace NYT::NLsm {

////////////////////////////////////////////////////////////////////////////////

class TTablet
    : public virtual TRefCounted
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TTabletId, Id);
    DEFINE_BYREF_RW_PROPERTY(NYPath::TYPath, TablePath);
    DEFINE_BYVAL_RW_PROPERTY(TTabletCellId, CellId);
    DEFINE_BYREF_RW_PROPERTY(std::string, TabletCellBundle);
    DEFINE_BYVAL_RW_PROPERTY(bool, PhysicallySorted);
    DEFINE_BYVAL_RW_PROPERTY(bool, Mounted);
    DEFINE_BYVAL_RW_PROPERTY(TTableMountConfigPtr, MountConfig);
    DEFINE_BYVAL_RW_PROPERTY(NHydra::TRevision, MountRevision);
    DEFINE_BYVAL_RW_PROPERTY(std::string, LoggingTag);

    DEFINE_BYVAL_RW_PROPERTY(bool, IsForcedRotationPossible);
    DEFINE_BYVAL_RW_PROPERTY(bool, IsOverflowRotationNeeded);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TInstant>, LastPeriodicRotationTime);
    DEFINE_BYVAL_RW_PROPERTY(bool, IsOutOfBandRotationRequested);
    DEFINE_BYVAL_RW_PROPERTY(bool, IsCompactionAllowed);

    DEFINE_BYREF_RW_PROPERTY(TTabletLsmStatistics, LsmStatistics);

    using THunkChunkMap = THashMap<NChunkClient::TChunkId, std::unique_ptr<THunkChunk>>;
    DEFINE_BYREF_RW_PROPERTY(THunkChunkMap, HunkChunks);

    using TStoreIdMap = THashMap<TStoreId, TStore*>;
    DEFINE_BYREF_RW_PROPERTY(TStoreIdMap, StoreIdMap);

    // Sorted.
    DEFINE_BYREF_RW_PROPERTY(std::vector<std::unique_ptr<TPartition>>, Partitions);
    DEFINE_BYREF_RW_PROPERTY(std::unique_ptr<TPartition>, Eden);
    DEFINE_BYVAL_RW_PROPERTY(int, OverlappingStoreCount);
    DEFINE_BYVAL_RW_PROPERTY(int, EdenOverlappingStoreCount);
    DEFINE_BYVAL_RW_PROPERTY(int, CriticalPartitionCount);
    DEFINE_BYVAL_RW_PROPERTY(bool, HasTtlColumn);

    // Ordered.
    DEFINE_BYREF_RW_PROPERTY(std::vector<std::unique_ptr<TStore>>, Stores);

public:
    bool IsPhysicallySorted() const;

    TStore* FindActiveStore() const;
    TStore* GetStore(TStoreId id) const;

    void CopyMetaFrom(const TTablet* tablet);

    void Persist(const TStreamPersistenceContext& context);
};

DEFINE_REFCOUNTED_TYPE(TTablet)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
