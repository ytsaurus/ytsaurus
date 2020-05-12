#pragma once

#include "public.h"
#include "dynamic_store_bits.h"
#include "store.h"

#include <yt/server/node/cluster_node/public.h>

#include <yt/server/node/data_node/public.h>

#include <yt/client/table_client/schema.h>

#include <yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/core/actions/signal.h>

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/logging/log.h>

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
    virtual TStoreId GetId() const override;
    virtual TTablet* GetTablet() const override;

    virtual EStoreState GetStoreState() const override;
    virtual void SetStoreState(EStoreState state) override;

    void SetMemoryTracker(NNodeTrackerClient::TNodeMemoryTrackerPtr memoryTracker);
    virtual i64 GetDynamicMemoryUsage() const override;

    virtual void Save(TSaveContext& context) const override;
    virtual void Load(TLoadContext& context) override;

    virtual void BuildOrchidYson(NYTree::TFluentMap fluent) override;

protected:
    const TTabletManagerConfigPtr Config_;
    const TTabletChunkReaderConfigPtr ReaderConfig_;
    const TStoreId StoreId_;
    TTablet* const Tablet_;

    const TTabletPerformanceCountersPtr PerformanceCounters_;
    const TRuntimeTabletDataPtr RuntimeData_;
    const TTabletId TabletId_;
    const NYPath::TYPath TablePath_;
    const NTableClient::TTableSchema Schema_;
    const int KeyColumnCount_;
    const int SchemaColumnCount_;
    const int ColumnLockCount_;
    const std::vector<TString> LockIndexToName_;
    const std::vector<int> ColumnIndexToLockIndex_;

    EStoreState StoreState_ = EStoreState::Undefined;

    const NLogging::TLogger Logger;

    NNodeTrackerClient::TNodeMemoryTrackerPtr MemoryTracker_;
    NNodeTrackerClient::TNodeMemoryTrackerGuard DynamicMemoryTrackerGuard_;


    TOwningKey RowToKey(TUnversionedRow row) const;
    TOwningKey RowToKey(TSortedDynamicRow row) const;

    virtual NNodeTrackerClient::EMemoryCategory GetMemoryCategory() const = 0;

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
    virtual TTimestamp GetMinTimestamp() const override;
    virtual TTimestamp GetMaxTimestamp() const override;

    //! Sets the store state, as expected.
    //! Additionally, when the store transitions from |ActiveDynamic| to |PassiveDynamic|,
    //! invokes #OnSetPassive.
    virtual void SetStoreState(EStoreState state);

    virtual i64 GetCompressedDataSize() const override;
    virtual i64 GetUncompressedDataSize() const override;

    // IDynamicStore implementation.
    virtual EStoreFlushState GetFlushState() const override;
    virtual void SetFlushState(EStoreFlushState state) override;

    virtual i64 GetValueCount() const override;
    virtual i64 GetLockCount() const override;

    virtual i64 GetPoolSize() const;
    virtual i64 GetPoolCapacity() const;

    virtual TInstant GetLastFlushAttemptTimestamp() const override;
    virtual void UpdateFlushAttemptTimestamp() override;

    virtual void BuildOrchidYson(NYTree::TFluentMap fluent) override;

    virtual bool IsDynamic() const override;
    virtual IDynamicStorePtr AsDynamic() override;

protected:
    //! Some sanity checks may need the tablet's atomicity mode but the tablet may die.
    //! So we capture a copy of this mode upon store's construction.
    const NTransactionClient::EAtomicity Atomicity_;

    const NTableClient::TRowBufferPtr RowBuffer_;

    TTimestamp MinTimestamp_ = NTransactionClient::MaxTimestamp;
    TTimestamp MaxTimestamp_ = NTransactionClient::MinTimestamp;

    EStoreFlushState FlushState_ = EStoreFlushState::None;
    TInstant LastFlushAttemptTimestamp_;

    i64 StoreLockCount_ = 0;
    i64 StoreValueCount_ = 0;

    void UpdateTimestampRange(TTimestamp commitTimestamp);

    virtual void OnSetPassive() = 0;

    virtual NNodeTrackerClient::EMemoryCategory GetMemoryCategory() const override;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TPreloadedBlockCache)

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
        TTimestamp chunkTimestamp,
        TTablet* tablet,
        NChunkClient::IBlockCachePtr blockCache,
        NDataNode::TChunkRegistryPtr chunkRegistry,
        NDataNode::TChunkBlockManagerPtr chunkBlockManager,
        TVersionedChunkMetaManagerPtr chunkMetaManager,
        NApi::NNative::IClientPtr client,
        const NNodeTrackerClient::TNodeDescriptor& localDescriptor);

    virtual void Initialize(const NTabletNode::NProto::TAddStoreDescriptor* descriptor);

    const NChunkClient::NProto::TChunkMeta& GetChunkMeta() const;

    TInstant GetCreationTime() const;

    // IStore implementation.
    virtual TTimestamp GetMinTimestamp() const override;
    virtual TTimestamp GetMaxTimestamp() const override;

    virtual i64 GetCompressedDataSize() const override;
    virtual i64 GetUncompressedDataSize() const override;
    virtual i64 GetRowCount() const override;

    virtual TCallback<void(TSaveContext&)> AsyncSave() override;
    virtual void AsyncLoad(TLoadContext& context) override;

    virtual void BuildOrchidYson(NYTree::TFluentMap fluent) override;

    // IChunkStore implementation.
    virtual void SetBackingStore(IDynamicStorePtr store) override;
    virtual bool HasBackingStore() const override;
    virtual IDynamicStorePtr GetBackingStore() override;

    virtual EStorePreloadState GetPreloadState() const override;
    virtual void SetPreloadState(EStorePreloadState state) override;

    virtual bool IsPreloadAllowed() const override;
    virtual void UpdatePreloadAttempt(bool isBackoff) override;

    virtual TFuture<void> GetPreloadFuture() const override;
    virtual void SetPreloadFuture(TFuture<void> future) override;

    virtual EStoreCompactionState GetCompactionState() const override;
    virtual void SetCompactionState(EStoreCompactionState state) override;

    virtual bool IsCompactionAllowed() const override;
    virtual void UpdateCompactionAttempt() override;

    virtual bool IsChunk() const override;
    virtual IChunkStorePtr AsChunk() override;

    virtual TReaders GetReaders(const NConcurrency::IThroughputThrottlerPtr& throttler) override;

    virtual NTabletClient::EInMemoryMode GetInMemoryMode() const override;
    virtual void SetInMemoryMode(NTabletClient::EInMemoryMode mode) override;

    virtual void Preload(TInMemoryChunkDataPtr chunkData) override;

    virtual NChunkClient::TChunkId GetChunkId() const override;

protected:
    const NChunkClient::IBlockCachePtr BlockCache_;
    const NDataNode::TChunkRegistryPtr ChunkRegistry_;
    const NDataNode::TChunkBlockManagerPtr ChunkBlockManager_;
    const TVersionedChunkMetaManagerPtr ChunkMetaManager_;
    const NApi::NNative::IClientPtr Client_;
    const NNodeTrackerClient::TNodeDescriptor LocalDescriptor_;

    NTabletClient::EInMemoryMode InMemoryMode_ = NTabletClient::EInMemoryMode::None;
    EStorePreloadState PreloadState_ = EStorePreloadState::None;
    TInstant AllowedPreloadTimestamp_;
    TFuture<void> PreloadFuture_;
    TPreloadedBlockCachePtr PreloadedBlockCache_;
    NTableClient::TChunkStatePtr ChunkState_;

    EStoreCompactionState CompactionState_ = EStoreCompactionState::None;
    TInstant AllowedCompactionTimestamp_;

    NConcurrency::TReaderWriterSpinLock SpinLock_;
    std::atomic<NProfiling::TCpuInstant> LocalChunkCheckDeadline_ = {0};
    NChunkClient::IChunkReaderPtr CachedChunkReader_;
    NTableClient::ILookupReaderPtr CachedLookupReader_;
    bool CachedReadersLocal_ = false;
    TWeakPtr<NDataNode::IChunk> CachedWeakChunk_;

    // Cached for fast retrieval from ChunkMeta_.
    NChunkClient::NProto::TMiscExt MiscExt_;
    NChunkClient::TRefCountedChunkMetaPtr ChunkMeta_;

    NChunkClient::TChunkId ChunkId_;

    TTimestamp ChunkTimestamp_;

    void OnLocalReaderFailed();

    NChunkClient::IBlockCachePtr GetBlockCache();

    virtual void PrecacheProperties();
    virtual NNodeTrackerClient::EMemoryCategory GetMemoryCategory() const override;

    bool ValidateBlockCachePreloaded();

    virtual NTableClient::TKeyComparer GetKeyComparer() = 0;

private:
    IDynamicStorePtr BackingStore_;

    NChunkClient::IBlockCachePtr DoGetBlockCache();

    friend TPreloadedBlockCache;
};

////////////////////////////////////////////////////////////////////////////////

class TSortedStoreBase
    : public ISortedStore
{
public:
    virtual TPartition* GetPartition() const override;
    virtual void SetPartition(TPartition* partition) override;

    virtual bool IsSorted() const override;
    virtual ISortedStorePtr AsSorted() override;

protected:
    TPartition* Partition_ = nullptr;

};

////////////////////////////////////////////////////////////////////////////////

class TOrderedStoreBase
    : public IOrderedStore
{
public:
    virtual bool IsOrdered() const override;
    virtual IOrderedStorePtr AsOrdered() override;

    virtual i64 GetStartingRowIndex() const override;
    virtual void SetStartingRowIndex(i64 value) override;

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);

protected:
    i64 StartingRowIndex_ = 0;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
