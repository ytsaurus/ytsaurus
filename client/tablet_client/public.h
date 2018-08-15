#pragma once

#include <yt/core/misc/public.h>

#include <yt/client/hydra/public.h>

#include <yt/client/object_client/public.h>

namespace NYT {
namespace NTabletClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETabletState,
    // Individual states
    ((Mounting)        (0))
    ((Mounted)         (1))
    ((Unmounting)      (2))
    ((Unmounted)       (3))
    ((Freezing)        (4))
    ((Frozen)          (5))
    ((Unfreezing)      (6))
    ((FrozenMounting)  (7))

    // Aggregated states
    ((None)          (100))
    ((Mixed)         (101))
);

constexpr ETabletState MinValidTabletState = ETabletState::Mounting;
constexpr ETabletState MaxValidTabletState = ETabletState::FrozenMounting;

// Keep in sync with NRpcProxy::NProto::ETableReplicaMode.
DEFINE_ENUM(ETableReplicaMode,
    ((Sync)     (0))
    ((Async)    (1))
);

DEFINE_ENUM(EErrorCode,
    ((TransactionLockConflict)  (1700))
    ((NoSuchTablet)             (1701))
    ((TabletNotMounted)         (1702))
    ((AllWritesDisabled)        (1703))
    ((InvalidMountRevision)     (1704))
    ((TableReplicaAlreadyExists)(1705))
);

constexpr int TypicalPeerCount = 5;
constexpr int MaxPeerCount = 10;

DEFINE_ENUM(EInMemoryMode,
    ((None)        (0))
    ((Compressed)  (1))
    ((Uncompressed)(2))
);

using TTabletCellId = NHydra::TCellId;
extern const TTabletCellId NullTabletCellId;

using TTabletId = NObjectClient::TObjectId;
extern const TTabletId NullTabletId;

using TStoreId = NObjectClient::TObjectId;
extern const TStoreId NullStoreId;

using TPartitionId = NObjectClient::TObjectId;
extern const TPartitionId NullPartitionId;

using TTabletCellBundleId = NObjectClient::TObjectId;
extern const TTabletCellBundleId NullTabletCellBundleId;

using TTableReplicaId = NObjectClient::TObjectId;

using TTabletActionId = NObjectClient::TObjectId;

DEFINE_BIT_ENUM(EReplicationLogDataFlags,
    ((None)      (0x0000))
    ((Missing)   (0x0001))
    ((Aggregate) (0x0002))
);

struct TReplicationLogTable
{
    static const TString ChangeTypeColumnName;
    static const TString KeyColumnNamePrefix;
    static const TString ValueColumnNamePrefix;
    static const TString FlagsColumnNamePrefix;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTableMountCacheConfig)

DECLARE_REFCOUNTED_STRUCT(TTableMountInfo)
DECLARE_REFCOUNTED_STRUCT(TTabletInfo)
DECLARE_REFCOUNTED_STRUCT(TTableReplicaInfo)
DECLARE_REFCOUNTED_STRUCT(ITableMountCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT

