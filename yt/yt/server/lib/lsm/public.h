#pragma once

#include <yt/yt/server/lib/tablet_node/public.h>

namespace NYT::NLsm {

////////////////////////////////////////////////////////////////////////////////

using NTabletNode::TTabletCellId;
using NTabletNode::NullTabletCellId;
using NTabletNode::TTabletId;
using NTabletNode::NullTabletId;
using NTabletNode::TStoreId;
using NTabletNode::TDynamicStoreId;
using NTabletNode::NullStoreId;
using NTabletNode::TPartitionId;
using NTabletNode::NullPartitionId;

using NTabletNode::EStoreType;
using NTabletNode::EStoreState;
using NTabletNode::EStoreFlushState;
using NTabletNode::EStorePreloadState;
using NTabletNode::EStoreCompactionState;

using NTabletNode::EPartitionState;

using NTabletNode::TTimestamp;

using NTabletNode::EdenIndex;

using NTabletNode::TTableMountConfigPtr;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ILsmBackend)

DECLARE_REFCOUNTED_CLASS(TTablet)

class TPartition;
class TStore;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EStoreCompactionReason,
    (None)
    (Regular)
    (Forced)
    (Periodic)
    (StoreOutOfTabletRange)
    (DiscardByTtl)
    (TooManyTimestamps)
    (TtlCleanupExpected)
    (NarrowChunkView)
);

DEFINE_ENUM(EStoreRotationReason,
    (None)
    (Forced)
    (Periodic)
    (Overflow)
    (OutOfBand)
    (Discard)
);

DEFINE_ENUM(EStoreCompactorActivityKind,
    (Compaction)
    (Partitioning)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLsm
