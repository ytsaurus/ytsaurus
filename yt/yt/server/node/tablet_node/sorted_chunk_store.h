#pragma once

#include "public.h"
#include "store_detail.h"

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/client/chunk_client/read_limit.h>

#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/versioned_row.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TSortedChunkStore
    : public TChunkStoreBase
    , public TSortedStoreBase
{
public:
    TSortedChunkStore(
        TTabletManagerConfigPtr config,
        TStoreId id,
        NChunkClient::TChunkId chunkId,
        const NChunkClient::TReadRange& readRange,
        TTimestamp chunkTimestamp,
        TTablet* tablet,
        NChunkClient::IBlockCachePtr blockCache,
        NDataNode::TChunkRegistryPtr chunkRegistry = nullptr,
        NDataNode::TChunkBlockManagerPtr chunkBlockManager = nullptr,
        TVersionedChunkMetaManagerPtr chunkMetaManager = nullptr,
        NApi::NNative::IClientPtr client = nullptr,
        const NNodeTrackerClient::TNodeDescriptor& localDescriptor = NNodeTrackerClient::TNodeDescriptor());
    ~TSortedChunkStore();

    // IStore implementation.
    virtual EStoreType GetType() const override;

    virtual TSortedChunkStorePtr AsSortedChunk() override;

    virtual void BuildOrchidYson(NYTree::TFluentMap fluent) override;

    // ISortedStore implementation.
    virtual TOwningKey GetMinKey() const override;
    virtual TOwningKey GetUpperBoundKey() const override;

    virtual NTableClient::IVersionedReaderPtr CreateReader(
        const TTabletSnapshotPtr& tabletSnapshot,
        TSharedRange<NTableClient::TRowRange> bounds,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TColumnFilter& columnFilter,
        const NChunkClient::TClientBlockReadOptions& blockReadOptions,
        NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler()) override;

    virtual NTableClient::IVersionedReaderPtr CreateReader(
        const TTabletSnapshotPtr& tabletSnapshot,
        const TSharedRange<TKey>& keys,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TColumnFilter& columnFilter,
        const NChunkClient::TClientBlockReadOptions& blockReadOptions,
        NConcurrency::IThroughputThrottlerPtr throttler = NConcurrency::GetUnlimitedThrottler()) override;

    virtual bool CheckRowLocks(
        TUnversionedRow row,
        TLockMask lockMask,
        TWriteContext* context) override;

    virtual void Save(TSaveContext& context) const override;
    virtual void Load(TLoadContext& context) override;

private:
    // Cached for fast retrieval from ChunkMeta_.
    TOwningKey MinKey_;
    TOwningKey UpperBoundKey_;

    TSharedRange<NTableClient::TRowRange> ReadRange_;

    const NTableClient::TKeyComparer KeyComparer_;

    TSharedRange<TKey> FilterKeysByReadRange(
        const TSharedRange<TKey>& keys,
        int* skippedBefore,
        int* skippedAfter) const;

    TSharedRange<NTableClient::TRowRange> FilterRowRangesByReadRange(
        const TSharedRange<NTableClient::TRowRange>& ranges) const;

    NTableClient::IVersionedReaderPtr CreateCacheBasedReader(
        const TSharedRange<TKey>& keys,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TColumnFilter& columnFilter,
        const NChunkClient::TClientBlockReadOptions& blockReadOptions,
        const TTableSchema& schema);
    NTableClient::IVersionedReaderPtr CreateCacheBasedReader(
        TSharedRange<NTableClient::TRowRange> bounds,
        TTimestamp timestamp,
        bool produceAllVersions,
        const TColumnFilter& columnFilter,
        const NChunkClient::TClientBlockReadOptions& blockReadOptions,
        const TTableSchema& schema,
        const TSharedRange<NTableClient::TRowRange>& singletonClippingRange);

    NTableClient::TChunkStatePtr PrepareChunkState(
        NChunkClient::IChunkReaderPtr chunkReader,
        const NChunkClient::TClientBlockReadOptions& blockReadOptions);

    void ValidateBlockSize(
        const TTabletSnapshotPtr& tabletSnapshot,
        const NTableClient::TChunkStatePtr& chunkState,
        const TWorkloadDescriptor& workloadDescriptor);

    virtual void PrecacheProperties() override;

    virtual NTableClient::TKeyComparer GetKeyComparer() override;

    ISortedStorePtr GetSortedBackingStore();

    bool HasNontrivialReadRange() const;
};

DEFINE_REFCOUNTED_TYPE(TSortedChunkStore)

////////////////////////////////////////////////////////////////////////////////

//! Returns the slice of |keys| falling into the half-interval |readRange|
//! and the number of skipped keys at the beginning and at the end.
TSharedRange<TKey> FilterKeysByReadRange(
    const NTableClient::TRowRange& readRange,
    const TSharedRange<TKey>& keys,
    int* skippedBefore,
    int* skippedAfter);

//! Returns the slice of |ranges| having non-empty intersection with the half-interval |readRange|.
TSharedRange<NTableClient::TRowRange> FilterRowRangesByReadRange(
    const NTableClient::TRowRange& readRange,
    const TSharedRange<NTableClient::TRowRange>& ranges);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
