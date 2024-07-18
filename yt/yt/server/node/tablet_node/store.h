#pragma once

#include "public.h"

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/client/table_client/row_base.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/table_client/hunks.h>

#include <yt/yt/core/actions/signal.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/range.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct THunkChunkRef
{
    THunkChunkPtr HunkChunk;
    i64 HunkCount = 0;
    i64 TotalHunkLength = 0;

    NTableClient::THunkChunkRef ToBase() const;
};

void Serialize(const THunkChunkRef& ref, NYson::IYsonConsumer* consumer);

void FormatValue(TStringBuilderBase* builder, const THunkChunkRef& ref, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

struct TBackendReaders
{
    NChunkClient::IChunkReaderPtr ChunkReader;
    NTableClient::IOffloadingReaderPtr OffloadingReader;
    TTabletStoreReaderConfigPtr ReaderConfig;
};

////////////////////////////////////////////////////////////////////////////////

struct IStore
    : public virtual TRefCounted
{
    virtual EStoreType GetType() const = 0;

    virtual bool IsDynamic() const;
    virtual IDynamicStorePtr AsDynamic();

    virtual bool IsChunk() const;
    virtual IChunkStorePtr AsChunk();

    virtual bool IsSorted() const;
    virtual ISortedStorePtr AsSorted();
    virtual TSortedDynamicStorePtr AsSortedDynamic();
    virtual TSortedChunkStorePtr AsSortedChunk();

    virtual bool IsOrdered() const;
    virtual IOrderedStorePtr AsOrdered();
    virtual TOrderedDynamicStorePtr AsOrderedDynamic();
    virtual TOrderedChunkStorePtr AsOrderedChunk();

    virtual TStoreId GetId() const = 0;
    virtual TTablet* GetTablet() const = 0;

    virtual i64 GetDataWeight() const = 0;
    virtual i64 GetCompressedDataSize() const = 0;
    virtual i64 GetUncompressedDataSize() const = 0;
    virtual i64 GetRowCount() const = 0;
    virtual bool IsEmpty() const = 0;

    //! Returns the minimum timestamp of changes recorded in the store.
    virtual TTimestamp GetMinTimestamp() const = 0;

    //! Returns the maximum timestamp of changes recorded in the store.
    virtual TTimestamp GetMaxTimestamp() const = 0;

    virtual EStoreState GetStoreState() const = 0;
    virtual void SetStoreState(EStoreState state) = 0;

    //! Initializes the store after construction.
    /*!
     *  Two scenarios are possible:
     *
     *  1) The store is being instantiated in a mutation.
     *  The caller must provide store descriptor to the constructor and invoke
     *  #Initialize right away.
     *
     *  2) The store is being loaded from a snapshot.
     *  The caller must provide null store descriptor to the constructor and
     *  invoke #Initialize once the store's state is fully loaded (see #AsyncLoad).
     */
    virtual void Initialize() = 0;

    //! Returns the number of dynamic bytes currently used by the store.
    virtual i64 GetDynamicMemoryUsage() const = 0;

    //! Serializes the synchronous part of the state.
    virtual void Save(TSaveContext& context) const = 0;
    //! Deserializes the synchronous part of the state.
    virtual void Load(TLoadContext& context) = 0;

    //! Serializes the asynchronous part of the state.
    virtual TCallback<void(TSaveContext&)> AsyncSave() = 0;
    //! Deserializes the asynchronous part of the state.
    virtual void AsyncLoad(TLoadContext& context) = 0;

    virtual void BuildOrchidYson(NYTree::TFluentMap fluent) = 0;

    //! Fills store descriptor to replicate to another tablet servant.
    virtual void PopulateAddStoreDescriptor(NProto::TAddStoreDescriptor* descriptor) = 0;
};

DEFINE_REFCOUNTED_TYPE(IStore)

////////////////////////////////////////////////////////////////////////////////

struct IDynamicStore
    : public virtual IStore
{
    virtual i64 GetValueCount() const = 0;
    virtual i64 GetTimestampCount() const = 0;
    virtual i64 GetLockCount() const = 0;

    virtual i64 GetPoolSize() const = 0;
    virtual i64 GetPoolCapacity() const = 0;

    virtual EStoreFlushState GetFlushState() const = 0;
    virtual void SetFlushState(EStoreFlushState state) = 0;

    virtual TInstant GetLastFlushAttemptTimestamp() const = 0;
    virtual void UpdateFlushAttemptTimestamp() = 0;

    virtual void SetBackupCheckpointTimestamp(TTimestamp timestamp) = 0;

    virtual void LockHunkStores(const NTableClient::THunkChunksInfo& hunkChunksInfo) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDynamicStore)

////////////////////////////////////////////////////////////////////////////////

struct IChunkStore
    : public virtual IStore
{
    virtual TInstant GetCreationTime() const = 0;

    virtual void SetBackingStore(IDynamicStorePtr store) = 0;
    virtual IDynamicStorePtr GetBackingStore() const = 0;

    virtual EStorePreloadState GetPreloadState() const = 0;
    virtual void SetPreloadState(EStorePreloadState state) = 0;

    virtual bool IsPreloadAllowed() const = 0;
    virtual void UpdatePreloadAttempt(bool isBackoff) = 0;

    virtual TFuture<void> GetPreloadFuture() const = 0;
    virtual void SetPreloadFuture(TFuture<void> future) = 0;

    virtual TBackendReaders GetBackendReaders(
        std::optional<EWorkloadCategory> workloadCategory) = 0;
    virtual void InvalidateCachedReaders(const TTableSettings& settings) = 0;
    virtual NChunkClient::TChunkReplicaWithMediumList GetReplicas(
        NNodeTrackerClient::TNodeId localNodeId) = 0;

    virtual NTabletClient::EInMemoryMode GetInMemoryMode() const = 0;
    virtual void SetInMemoryMode(NTabletClient::EInMemoryMode mode) = 0;

    virtual void Preload(TInMemoryChunkDataPtr chunkData) = 0;

    virtual EStoreCompactionState GetCompactionState() const = 0;
    virtual void SetCompactionState(EStoreCompactionState state) = 0;

    virtual void UpdateCompactionAttempt() = 0;
    virtual TInstant GetLastCompactionTimestamp() const = 0;

    virtual NChunkClient::TChunkId GetChunkId() const = 0;
    //! Returns the timestamp provided by the chunk view, if any, and NullTimestamp otherwise.
    virtual TTimestamp GetOverrideTimestamp() const = 0;

    virtual const NChunkClient::NProto::TChunkMeta& GetChunkMeta() const = 0;

    virtual const std::vector<THunkChunkRef>& HunkChunkRefs() const = 0;

    virtual i64 GetMemoryUsage() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkStore)

////////////////////////////////////////////////////////////////////////////////

struct ISortedStore
    : public virtual IStore
{
    virtual TPartition* GetPartition() const = 0;
    virtual void SetPartition(TPartition* partition) = 0;

    //! Returns the minimum key in the store, inclusive.
    //! Result is guaranteed to have the same width as schema key columns.
    virtual TLegacyOwningKey GetMinKey() const = 0;

    //! Returns the maximum key in the store, exclusive.
    //! Result is NOT guaranteed to have the same width as schema key columns,
    //! it can be wider or narrower and contain sentinel values.
    //!
    //! Motivation: it is impossible to get exclusive upper bound of chunk keys
    //! having the same width as schema key columns, and it is necessary to have
    //! the upper bound exclusive because such bounds come from chunk view.
    virtual TLegacyOwningKey GetUpperBoundKey() const = 0;

    //! Returns true if min/upper bound keys were overridden and do not match boundary
    //! keys from chunk meta. In particular, always returns false for dynamic stores.
    virtual bool HasNontrivialReadRange() const = 0;

    //! Creates a reader for the range from |lowerKey| (inclusive) to |upperKey| (exclusive).
    /*!
    *  If no matching row is found then |nullptr| might be returned.
    *
    *  The reader will be providing values filtered by |timestamp| and columns
    *  filtered by |columnFilter|.
    *
    *  Depending on |produceAllVersions| flag reader would either provide all value versions
    *  or just the last one.
    *
    *  This call is typically synchronous and fast but may occasionally yield.
    *
    *  Thread affinity: any
    */
    virtual NTableClient::IVersionedReaderPtr CreateReader(
        const TTabletSnapshotPtr& tabletSnapshot,
        TSharedRange<NTableClient::TRowRange> bounds,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TColumnFilter& columnFilter,
        const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
        std::optional<EWorkloadCategory> workloadCategory) = 0;

    //! Creates a reader for the set of |keys|.
    /*!
    *  If no matching row is found then |nullptr| might be returned.
    *
    *  The reader will be providing values filtered by |timestamp| and columns
    *  filtered by |columnFilter|.
    *
    *  Depending on |produceAllVersions| flag reader would either provide all value versions
    *  or just the last one.
    *
    *  This call is typically synchronous and fast but may occasionally yield.
    *
    *  Thread affinity: any
    */
    virtual NTableClient::IVersionedReaderPtr CreateReader(
        const TTabletSnapshotPtr& tabletSnapshot,
        TSharedRange<TLegacyKey> keys,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TColumnFilter& columnFilter,
        const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
        std::optional<EWorkloadCategory> workloadCategory) = 0;

    //! Checks that the transaction attempting to take locks indicated by #lockMask for #row
    //! has no conflicts within the store.
    /*!
     *  Returns |true| if no issues were found and |false| otherwise.
     *
     *  In the latter case either TWriteContext::Error is set (if lock conflict is discovered)
     *  of TWriteContext::BlockedStore and similar are set (if blocked row is encountered).
     *
     *  Thread affinity: any
     */
    virtual bool CheckRowLocks(
        TUnversionedRow row,
        NTableClient::TLockMask lockMask,
        TWriteContext* context) = 0;
};

DEFINE_REFCOUNTED_TYPE(ISortedStore)

////////////////////////////////////////////////////////////////////////////////

struct IOrderedStore
    : public virtual IStore
{
    //! Returns the starting row index for this store.
    virtual i64 GetStartingRowIndex() const = 0;

    //! Initializes the starting row index for this store.
    virtual void SetStartingRowIndex(i64 index) = 0;

    //! Creates a reader for the range from |lowerRowIndex| (inclusive) to |upperRowIndex| (exclusive).
    /*!
     *  If no matching row is found then |nullptr| might be returned.
     *
     *  Like in ISortedStore, #columnFilter enables filtering a subset of table columns.
     *  However these ids refer to the "extended" schema with prepended |(tablet_index, row_index)| columns.
     *
     *  #tabletIndex is needed to properly fill the |tablet_index| field, if requested so by #columnFilter.
     *
     *  This call is typically synchronous and fast but may occasionally yield.
     *
     *  Thread affinity: any
     */
    virtual NTableClient::ISchemafulUnversionedReaderPtr CreateReader(
        const TTabletSnapshotPtr& tabletSnapshot,
        int tabletIndex,
        i64 lowerRowIndex,
        i64 upperRowIndex,
        NTransactionClient::TTimestamp timestamp,
        const NTableClient::TColumnFilter& columnFilter,
        const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
        std::optional<EWorkloadCategory> workloadCategory) = 0;
};

DEFINE_REFCOUNTED_TYPE(IOrderedStore)

////////////////////////////////////////////////////////////////////////////////

struct TStoreIdFormatter
{
    void operator()(TStringBuilderBase* builder, const IStorePtr& store) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
