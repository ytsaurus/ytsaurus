#pragma once

#include "public.h"
#include "dynamic_store_bits.h"
#include "store.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/node/data_node/public.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TStoreBase
    : public virtual IStore
{
public:
    TStoreBase(
        TTabletManagerConfigPtr config,
        TStoreId id,
        TTablet* tablet);
    ~TStoreBase();

    // IStore implementation.
    TStoreId GetId() const override;
    TTablet* GetTablet() const override;

    bool IsEmpty() const override;

    EStoreState GetStoreState() const override;
    void SetStoreState(EStoreState state) override;

    void SetMemoryTracker(INodeMemoryTrackerPtr memoryTracker);
    i64 GetDynamicMemoryUsage() const override;

    void Initialize() override;

    void Save(TSaveContext& context) const override;
    void Load(TLoadContext& context) override;

    void BuildOrchidYson(NYTree::TFluentMap fluent) override;

    const NLogging::TLogger& GetLogger() const;

    TTabletId GetTabletId() const;

protected:
    const TTabletManagerConfigPtr Config_;
    const TStoreId StoreId_;
    TTablet* const Tablet_;

    const NTableClient::TTabletPerformanceCountersPtr PerformanceCounters_;
    const TRuntimeTabletDataPtr RuntimeData_;
    const TTabletId TabletId_;
    const NYPath::TYPath TablePath_;
    const NTableClient::TTableSchemaPtr Schema_;
    const int KeyColumnCount_;
    const int SchemaColumnCount_;
    const int ColumnLockCount_;
    const std::vector<TString> LockIndexToName_;
    const std::vector<int> ColumnIndexToLockIndex_;

    EStoreState StoreState_ = EStoreState::Undefined;

    const NLogging::TLogger Logger;

    INodeMemoryTrackerPtr MemoryTracker_;
    TMemoryUsageTrackerGuard DynamicMemoryTrackerGuard_;


    TLegacyOwningKey RowToKey(TUnversionedRow row) const;
    TLegacyOwningKey RowToKey(TSortedDynamicRow row) const;

    virtual EMemoryCategory GetMemoryCategory() const = 0;

    void SetDynamicMemoryUsage(i64 value);

private:
    i64 DynamicMemoryUsage_ = 0;

    static ETabletDynamicMemoryType DynamicMemoryTypeFromState(EStoreState state);
    void UpdateTabletDynamicMemoryUsage(i64 multiplier);
};

////////////////////////////////////////////////////////////////////////////////

class TDynamicStoreBase
    : public TStoreBase
    , public IDynamicStore
{
public:
    TDynamicStoreBase(
        TTabletManagerConfigPtr config,
        TStoreId id,
        TTablet* tablet);

    i64 Lock();
    i64 Unlock();

    // IStore implementation.
    TTimestamp GetMinTimestamp() const override;
    TTimestamp GetMaxTimestamp() const override;

    //! Sets the store state, as expected.
    //! Additionally, when the store transitions from |ActiveDynamic| to |PassiveDynamic|,
    //! invokes #OnSetPassive.
    void SetStoreState(EStoreState state) override;

    i64 GetDataWeight() const override;
    i64 GetCompressedDataSize() const override;
    i64 GetUncompressedDataSize() const override;

    // IDynamicStore implementation.
    EStoreFlushState GetFlushState() const override;
    void SetFlushState(EStoreFlushState state) override;

    i64 GetValueCount() const override;
    i64 GetLockCount() const override;

    i64 GetPoolSize() const override;
    i64 GetPoolCapacity() const override;

    TInstant GetLastFlushAttemptTimestamp() const override;
    void UpdateFlushAttemptTimestamp() override;

    void BuildOrchidYson(NYTree::TFluentMap fluent) override;

    bool IsDynamic() const override;
    IDynamicStorePtr AsDynamic() override;

    void SetBackupCheckpointTimestamp(TTimestamp timestamp) override;

    void LockHunkStores(const NTableClient::THunkChunksInfo& hunkChunksInfo) override;

protected:
    //! Some sanity checks may need the tablet's atomicity mode but the tablet may die.
    //! So we capture a copy of this mode upon store's construction.
    const NTransactionClient::EAtomicity Atomicity_;
    const NTableClient::TRowBufferPtr RowBuffer_;

    std::atomic<TTimestamp> MinTimestamp_ = NTransactionClient::MaxTimestamp;
    std::atomic<TTimestamp> MaxTimestamp_ = NTransactionClient::MinTimestamp;

    EStoreFlushState FlushState_ = EStoreFlushState::None;
    TInstant LastFlushAttemptTimestamp_;

    bool OutOfBandRotationRequested_ = false;

    i64 StoreLockCount_ = 0;
    i64 StoreValueCount_ = 0;

    void UpdateTimestampRange(TTimestamp commitTimestamp);

    virtual void OnSetPassive() = 0;
    virtual void OnSetRemoved() = 0;

    EMemoryCategory GetMemoryCategory() const override;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkStoreBase
    : public TStoreBase
    , public IChunkStore
{
public:
    TChunkStoreBase(
        TTabletManagerConfigPtr config,
        TStoreId id,
        NChunkClient::TChunkId chunkId,
        TTimestamp overrideTimestamp,
        TTablet* tablet,
        const NTabletNode::NProto::TAddStoreDescriptor* addStoreDescriptor,
        NChunkClient::IBlockCachePtr blockCache,
        IVersionedChunkMetaManagerPtr chunkMetaManager,
        IBackendChunkReadersHolderPtr backendReadersHolder);

    void Initialize() override;

    // IStore implementation.
    TTimestamp GetMinTimestamp() const override;
    TTimestamp GetMaxTimestamp() const override;

    NErasure::ECodec GetErasureCodecId() const;
    bool IsStripedErasure() const;

    i64 GetDataWeight() const override;
    i64 GetCompressedDataSize() const override;
    i64 GetUncompressedDataSize() const override;
    i64 GetRowCount() const override;

    void Save(TSaveContext& context) const override;
    void Load(TLoadContext& context) override;

    TCallback<void(TSaveContext&)> AsyncSave() override;
    void AsyncLoad(TLoadContext& context) override;

    void BuildOrchidYson(NYTree::TFluentMap fluent) override;

    // IChunkStore implementation.
    TInstant GetCreationTime() const override;

    void SetBackingStore(IDynamicStorePtr store) override;
    IDynamicStorePtr GetBackingStore() const override;

    EStorePreloadState GetPreloadState() const override;
    void SetPreloadState(EStorePreloadState state) override;

    bool IsPreloadAllowed() const override;
    void UpdatePreloadAttempt(bool isBackoff) override;

    TFuture<void> GetPreloadFuture() const override;
    void SetPreloadFuture(TFuture<void> future) override;

    EStoreCompactionState GetCompactionState() const override;
    void SetCompactionState(EStoreCompactionState state) override;

    void UpdateCompactionAttempt() override;
    TInstant GetLastCompactionTimestamp() const override;

    bool IsChunk() const override;
    IChunkStorePtr AsChunk() override;

    TBackendReaders GetBackendReaders(
        std::optional<EWorkloadCategory> workloadCategory) override;
    void InvalidateCachedReaders(
        const TTableSettings& settings) override;
    NChunkClient::TChunkReplicaWithMediumList GetReplicas(
        NNodeTrackerClient::TNodeId localNodeId) override;

    NTabletClient::EInMemoryMode GetInMemoryMode() const override;
    void SetInMemoryMode(NTabletClient::EInMemoryMode mode) override;

    void Preload(TInMemoryChunkDataPtr chunkData) override;

    NChunkClient::TChunkId GetChunkId() const override;
    TTimestamp GetOverrideTimestamp() const override;

    const NChunkClient::NProto::TChunkMeta& GetChunkMeta() const override;

    const std::vector<THunkChunkRef>& HunkChunkRefs() const override;
    i64 GetMemoryUsage() const override;

    NChunkClient::IBlockCachePtr GetBlockCache();

    // Fast path.
    NTableClient::TCachedVersionedChunkMetaPtr FindCachedVersionedChunkMeta(
        bool prepareColumnarMeta);

    // Slow path.
    TFuture<NTableClient::TCachedVersionedChunkMetaPtr> GetCachedVersionedChunkMeta(
        const NChunkClient::IChunkReaderPtr& chunkReader,
        const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
        bool prepareColumnarMeta);

protected:
    const NChunkClient::IBlockCachePtr BlockCache_;
    const IVersionedChunkMetaManagerPtr ChunkMetaManager_;

    std::vector<THunkChunkRef> HunkChunkRefs_;

    // NB: These fields are accessed under SpinLock_.
    NTabletClient::EInMemoryMode InMemoryMode_ = NTabletClient::EInMemoryMode::None;
    EStorePreloadState PreloadState_ = EStorePreloadState::None;
    TInstant AllowedPreloadTimestamp_;
    TFuture<void> PreloadFuture_;
    TPreloadedBlockCachePtr PreloadedBlockCache_;
    NTableClient::TChunkStatePtr ChunkState_;

    EStoreCompactionState CompactionState_ = EStoreCompactionState::None;
    TInstant LastCompactionTimestamp_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, WeakCachedVersionedChunkMetaEntryLock_);
    TWeakPtr<TVersionedChunkMetaCacheEntry> WeakCachedVersionedChunkMetaEntry_;

    // Cached for fast retrieval from ChunkMeta_.
    NChunkClient::NProto::TMiscExt MiscExt_;
    NChunkClient::TRefCountedChunkMetaPtr ChunkMeta_;

    NChunkClient::TChunkId ChunkId_;

    TTimestamp OverrideTimestamp_;

    void OnLocalReaderFailed();

    EMemoryCategory GetMemoryCategory() const override;

    NTableClient::TChunkStatePtr FindPreloadedChunkState() const;

    virtual const NTableClient::TKeyComparer& GetKeyComparer() const = 0;

    TTabletStoreReaderConfigPtr GetReaderConfig();

private:
    const IBackendChunkReadersHolderPtr BackendReadersHolder_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    IDynamicStorePtr BackingStore_;

    NChunkClient::IBlockCachePtr DoGetBlockCache();
};

////////////////////////////////////////////////////////////////////////////////

class TSortedStoreBase
    : public ISortedStore
{
public:
    TPartition* GetPartition() const override;
    void SetPartition(TPartition* partition) override;

    bool IsSorted() const override;
    ISortedStorePtr AsSorted() override;

protected:
    TPartition* Partition_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

class TOrderedStoreBase
    : public IOrderedStore
{
public:
    bool IsOrdered() const override;
    IOrderedStorePtr AsOrdered() override;

    i64 GetStartingRowIndex() const override;
    void SetStartingRowIndex(i64 value) override;

    void Save(TSaveContext& context) const override;
    void Load(TLoadContext& context) override;

protected:
    i64 StartingRowIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

TLegacyOwningKey RowToKey(const TTableSchema& schema, TSortedDynamicRow row);

TTimestamp CalculateRetainedTimestamp(TTimestamp currentTimestamp, TDuration minDataTtl);

////////////////////////////////////////////////////////////////////////////////

//! This interface provides means of creating and holding appropriate backend chunk readers,
//! i.e. remote (replication and erasure) or local ones.
//! These readers are then used by readers of higher level.
//! This interface is helpful for unittesting of lookups and versioned chunk readers.
struct IBackendChunkReadersHolder
    : public TRefCounted
{
    //! #workloadCategory is used to pick relevant throughput throttlers.
    virtual TBackendReaders GetBackendReaders(
        TChunkStoreBase* owner,
        std::optional<EWorkloadCategory> workloadCategory) = 0;

    virtual NChunkClient::TChunkReplicaWithMediumList GetReplicas(
        TChunkStoreBase* owner,
        NNodeTrackerClient::TNodeId localNodeId) const = 0;

    //! Will reset config unless #config is null.
    virtual void InvalidateCachedReadersAndTryResetConfig(
        const TTabletStoreReaderConfigPtr& config) = 0;

    virtual TTabletStoreReaderConfigPtr GetReaderConfig() = 0;
};

DEFINE_REFCOUNTED_TYPE(IBackendChunkReadersHolder)

////////////////////////////////////////////////////////////////////////////////

IBackendChunkReadersHolderPtr CreateBackendChunkReadersHolder(
    IBootstrap* bootstrap,
    NApi::NNative::IClientPtr client,
    NNodeTrackerClient::TNodeDescriptor localNodeDescriptor,
    NDataNode::IChunkRegistryPtr chunkRegistry,
    TTabletStoreReaderConfigPtr readerConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
