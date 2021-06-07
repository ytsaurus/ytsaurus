#pragma once

#include <yt/yt/client/table_client/key.h>

#include <yt/yt/server/node/tablet_node/public.h>

#include "public.h"
#include "store.h"
#include "partition.h"

namespace NYT::NLsm {

////////////////////////////////////////////////////////////////////////////////

class TTablet
    : public virtual TRefCounted
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TTabletId, Id);
    DEFINE_BYVAL_RW_PROPERTY(TTabletCellId, CellId);
    DEFINE_BYVAL_RW_PROPERTY(bool, PhysicallySorted);
    DEFINE_BYVAL_RW_PROPERTY(bool, Mounted);
    DEFINE_BYVAL_RW_PROPERTY(TTableMountConfigPtr, MountConfig);
    DEFINE_BYVAL_RW_PROPERTY(NHydra::TRevision, MountRevision);
    DEFINE_BYVAL_RW_PROPERTY(NTabletNode::IPerTabletStructuredLoggerPtr, StructuredLogger);
    DEFINE_BYVAL_RW_PROPERTY(TString, LoggingTag);

    // Sorted.
    DEFINE_BYREF_RW_PROPERTY(std::vector<std::unique_ptr<TPartition>>, Partitions);
    DEFINE_BYREF_RW_PROPERTY(std::unique_ptr<TPartition>, Eden);
    DEFINE_BYVAL_RW_PROPERTY(int, OverlappingStoreCount);
    DEFINE_BYVAL_RW_PROPERTY(int, EdenOverlappingStoreCount);
    DEFINE_BYVAL_RW_PROPERTY(int, CriticalPartitionCount);

    bool IsPhysicallySorted() const;

    // Ordered.
    DEFINE_BYREF_RW_PROPERTY(std::vector<std::unique_ptr<TStore>>, Stores);

};

DEFINE_REFCOUNTED_TYPE(TTablet)

////////////////////////////////////////////////////////////////////////////////

} // NYT::NLsm
