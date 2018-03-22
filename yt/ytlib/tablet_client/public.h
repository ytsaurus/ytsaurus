#pragma once

#include <yt/ytlib/hydra/public.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/core/misc/public.h>

namespace NYT {
namespace NTabletClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TColumnFilter;
class TReqLookupRows;
class TReqRegisterTransactionActions;
class TRspRegisterTransactionActions;
class TTableReplicaStatistics;

} // namespace NProto

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
    (None)
    (Compressed)
    (Uncompressed)
);

////////////////////////////////////////////////////////////////////////////////

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

DEFINE_ENUM(ETabletBackgroundActivity,
    ((Compaction)     (0))
    ((Flush)          (1))
    ((Partitioning)   (2))
    ((Preload)        (3))
);

////////////////////////////////////////////////////////////////////////////////

//! Signatures enable checking tablet transaction integrity.
/*!
 *  When a transaction is created, its signature is #InitialTransactionSignature.
 *  Each change within a transaction is annotated with a signature; these signatures are
 *  added to the transaction's signature. For a commit to be successful, the final signature must
 *  be equal to #FinalTransactionSignature.
 */
using TTransactionSignature = ui32;
const TTransactionSignature InitialTransactionSignature = 0;
const TTransactionSignature FinalTransactionSignature = 0xffffffffU;

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

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TTableMountInfo)
DECLARE_REFCOUNTED_STRUCT(TTabletInfo)
DECLARE_REFCOUNTED_STRUCT(TTableReplicaInfo)
DECLARE_REFCOUNTED_STRUCT(ITableMountCache)

DECLARE_REFCOUNTED_CLASS(TTabletCellOptions)
DECLARE_REFCOUNTED_CLASS(TTabletCellConfig)
DECLARE_REFCOUNTED_CLASS(TTableMountCacheConfig)

class TWireProtocolReader;
class TWireProtocolWriter;

using TSchemaData = std::vector<ui32>;

DECLARE_REFCOUNTED_STRUCT(IWireProtocolRowsetReader)
DECLARE_REFCOUNTED_STRUCT(IWireProtocolRowsetWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT

