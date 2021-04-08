#pragma once

#include "public.h"
#include "store_detail.h"

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TOrderedChunkStore
    : public TChunkStoreBase
    , public TOrderedStoreBase
{
public:
    TOrderedChunkStore(
        NClusterNode::TBootstrap* bootstrap,
        TTabletManagerConfigPtr config,
        TStoreId id,
        TTablet* tablet,
        const NTabletNode::NProto::TAddStoreDescriptor* addStoreDescriptor,
        NChunkClient::IBlockCachePtr blockCache,
        NDataNode::IChunkRegistryPtr chunkRegistry = nullptr,
        NDataNode::IChunkBlockManagerPtr chunkBlockManager = nullptr,
        IVersionedChunkMetaManagerPtr chunkMetaManager = nullptr,
        NApi::NNative::IClientPtr client = nullptr,
        const NNodeTrackerClient::TNodeDescriptor& localDescriptor = {});

    // IStore implementation.
    virtual TOrderedChunkStorePtr AsOrderedChunk() override;

    // IChunkStore implementation.
    virtual EStoreType GetType() const override;

    // IOrderedStore implementation.
    virtual NTableClient::ISchemafulUnversionedReaderPtr CreateReader(
        const TTabletSnapshotPtr& tabletSnapshot,
        int tabletIndex,
        i64 lowerRowIndex,
        i64 upperRowIndex,
        const TColumnFilter& columnFilter,
        const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
        NConcurrency::IThroughputThrottlerPtr bandwidthThrottler = NConcurrency::GetUnlimitedThrottler()) override;

    virtual void Save(TSaveContext& context) const override;
    virtual void Load(TLoadContext& context) override;

private:
    class TReader;

    using TIdMapping = SmallVector<int, NTableClient::TypicalColumnCount>;

    virtual NTableClient::TKeyComparer GetKeyComparer() override;

    NTableClient::ISchemafulUnversionedReaderPtr TryCreateCacheBasedReader(
        const TColumnFilter& columnFilter,
        const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
        const NChunkClient::TReadRange& readRange,
        const NTableClient::TTableSchemaPtr& readSchema,
        bool enableTabletIndex,
        bool enableRowIndex,
        int tabletIndex,
        i64 lowerRowIndex,
        const TIdMapping& idMapping);
};

DEFINE_REFCOUNTED_TYPE(TOrderedChunkStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
