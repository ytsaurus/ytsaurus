#pragma once

#include <core/misc/common.h>
#include <core/misc/enum.h>

#include <ytlib/election/public.h>

#include <ytlib/tablet_client/public.h>

#include <ytlib/new_table_client/public.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/chunk_client/public.h>

#include <server/hydra/public.h>

#include <server/hive/public.h>
#include <ytlib/new_table_client/schema.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

using NElection::TCellId;
using NElection::NullCellId;

using NTabletClient::TTabletCellId;
using NTabletClient::NullTabletCellId;
using NTabletClient::TTabletId;
using NTabletClient::NullTabletId;
using NTabletClient::TStoreId;
using NTabletClient::NullStoreId;

using NTabletClient::ELockMode;
using NTabletClient::TTabletCellConfig;
using NTabletClient::TTabletCellConfigPtr;
using NTabletClient::TTabletCellOptions;
using NTabletClient::TTabletCellOptionsPtr;

using NTransactionClient::TTransactionId;
using NTransactionClient::NullTransactionId;

using NTransactionClient::TTimestamp;
using NTransactionClient::NullTimestamp;
using NTransactionClient::SyncLastCommittedTimestamp;
using NTransactionClient::AsyncLastCommittedTimestamp;
using NTransactionClient::AsyncAllCommittedTimestamp;

using NVersionedTableClient::EValueType;
using NVersionedTableClient::TKey;
using NVersionedTableClient::TOwningKey;
using NVersionedTableClient::TUnversionedValue;
using NVersionedTableClient::TVersionedValue;
using NVersionedTableClient::TUnversionedRow;
using NVersionedTableClient::TUnversionedOwningRow;
using NVersionedTableClient::TVersionedRow;
using NVersionedTableClient::TVersionedOwningRow;
using NVersionedTableClient::TColumnFilter;
using NVersionedTableClient::TTableSchema;
using NVersionedTableClient::TColumnSchema;

using NHive::ETransactionState;

////////////////////////////////////////////////////////////////////////////////

static const int TypicalStoreCount = 64;

DECLARE_ENUM(EPartitionState,
    (Normal)             // nothing special is happening
    (Splitting)          // split mutation is submitted
    (Merging)            // merge mutation is submitted
    (Compacting)         // compaction (or partitioning) is in progress 
    (Sampling)           // sampling is in progress
);

DECLARE_ENUM(ETabletState,
    // The only good state admitting read and write requests.
    (Mounted)

    // NB: All states below are for unmounting workflow only!
    (WaitingForLocks)
    (FlushPending)     // transient, transition to Flushing is pending
    (Flushing)
    (UnmountPending)   // transient, transition to Unmounted is pending
    (Unmounted)
);

DECLARE_ENUM(EStoreType,
    (DynamicMemory)
    (Chunk)
);

DECLARE_ENUM(EStoreState,
    (ActiveDynamic)         // dynamic, can receive updates
    (PassiveDynamic)        // dynamic, rotated and cannot receive more updates

    (Persistent)            // stored in a chunk

    (Flushing)              // transient, flush is in progress
    (FlushFailed)           // transient, waiting for back off to complete

    (Compacting)            // transient, compaction is in progress
    (CompactionFailed)      // transient, waiting for back off to complete

    (RemoveCommitting)      // UpdateTabletStores request sent
    (RemoveFailed)          // transient, waiting for back off to complete

    (Orphaned)              // belongs to a forcefully removed tablet
    (Removed)               // removed by rotation but still locked
);

DECLARE_ENUM(EAutomatonThreadQueue,
    (Default)
    (Read)
    (Write)
);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTabletHydraManageConfig)
DECLARE_REFCOUNTED_CLASS(TRetentionConfig)
DECLARE_REFCOUNTED_CLASS(TTableMountConfig)
DECLARE_REFCOUNTED_CLASS(TTransactionManagerConfig)
DECLARE_REFCOUNTED_CLASS(TTabletManagerConfig)
DECLARE_REFCOUNTED_CLASS(TStoreFlusherConfig)
DECLARE_REFCOUNTED_CLASS(TStoreCompactorConfig)
DECLARE_REFCOUNTED_CLASS(TPartitionBalancerConfig)
DECLARE_REFCOUNTED_CLASS(TTabletChunkReaderConfig)
DECLARE_REFCOUNTED_CLASS(TTabletNodeConfig)

DECLARE_REFCOUNTED_CLASS(TTabletSlotManager)
DECLARE_REFCOUNTED_CLASS(TTabletSlot)
DECLARE_REFCOUNTED_CLASS(TTabletAutomaton)

class TSaveContext;
class TLoadContext;

DECLARE_REFCOUNTED_CLASS(TTabletManager)
DECLARE_REFCOUNTED_CLASS(TTransactionManager)

class TPartition;
class TTablet;

DECLARE_REFCOUNTED_STRUCT(TPartitionSnapshot)
DECLARE_REFCOUNTED_STRUCT(TTabletSnapshot)

class TTransaction;

DECLARE_REFCOUNTED_STRUCT(IStore)

DECLARE_REFCOUNTED_CLASS(TDynamicMemoryStore)
DECLARE_REFCOUNTED_CLASS(TChunkStore)
DECLARE_REFCOUNTED_CLASS(TStoreManager)

struct TDynamicRowHeader;
class TDynamicRow;
struct TDynamicRowRef;

struct TEditListHeader;
template <class T>
class TEditList;
typedef TEditList<NVersionedTableClient::TVersionedValue> TValueList;
typedef TEditList<NVersionedTableClient::TTimestamp> TTimestampList;

class TUnversionedRowMerger;
class TVersionedRowMerger;

typedef NChunkClient::TMultiChunkWriterOptions TTabletWriterOptions;
typedef NChunkClient::TMultiChunkWriterOptionsPtr TTabletWriterOptionsPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
