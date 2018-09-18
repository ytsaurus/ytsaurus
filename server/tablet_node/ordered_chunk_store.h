#pragma once

#include "public.h"
#include "store_detail.h"

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TOrderedChunkStore
    : public TChunkStoreBase
    , public TOrderedStoreBase
{
public:
    TOrderedChunkStore(
        TTabletManagerConfigPtr config,
        const TStoreId& id,
        TTablet* tablet,
        NChunkClient::IBlockCachePtr blockCache,
        NDataNode::TChunkRegistryPtr chunkRegistry = nullptr,
        NDataNode::TChunkBlockManagerPtr chunkBlockManager = nullptr,
        TVersionedChunkMetaManagerPtr chunkMetaManager = nullptr,
        NApi::NNative::IClientPtr client = nullptr,
        const NNodeTrackerClient::TNodeDescriptor& localDescriptor = NNodeTrackerClient::TNodeDescriptor());
    ~TOrderedChunkStore();

    virtual void Initialize(const NTabletNode::NProto::TAddStoreDescriptor* descriptor) override;

    // IStore implementation.
    virtual TOrderedChunkStorePtr AsOrderedChunk() override;

    // IChunkStore implementation.
    virtual EStoreType GetType() const override;

    // IOrderedStore implementation.
    virtual NTableClient::ISchemafulReaderPtr CreateReader(
        const TTabletSnapshotPtr& tabletSnapshot,
        int tabletIndex,
        i64 lowerRowIndex,
        i64 upperRowIndex,
        const TColumnFilter& columnFilter,
        const NChunkClient::TClientBlockReadOptions& blockReadOptions) override;

private:
    class TReader;

    virtual NTableClient::TKeyComparer GetKeyComparer() override;
};

DEFINE_REFCOUNTED_TYPE(TOrderedChunkStore)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
