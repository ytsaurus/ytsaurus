#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/misc/ref.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Contains all relevant data (e.g. blocks) for in-memory chunks.
struct TInMemoryChunkData
    : public TIntrinsicRefCounted
{
    std::vector<TSharedRef> Blocks;
    EInMemoryMode InMemoryMode = EInMemoryMode::None;
};

DEFINE_REFCOUNTED_TYPE(TInMemoryChunkData)

////////////////////////////////////////////////////////////////////////////////

//! Manages in-memory tables served by the node.
/*!
 *  Ensures that chunk stores of in-memory tables are preloaded when a node starts.
 *
 *  Provides means for intercepting data write-out during flushes and compactions
 *  and thus enables new chunk stores to be created with all blocks already resident.
 */
class TInMemoryManager
    : public TRefCounted
{
public:
    TInMemoryManager(
        TInMemoryManagerConfigPtr config,
        NCellNode::TBootstrap* bootstrap);
    ~TInMemoryManager();

    NChunkClient::IBlockCachePtr CreateInterceptingBlockCache(EInMemoryMode mode);
    TInMemoryChunkDataPtr EvictInterceptedChunkData(const NChunkClient::TChunkId& chunkId);

private:
    class TImpl;
    using TImplPtr = TIntrusivePtr<TImpl>;
    const TImplPtr Impl_;

};

DEFINE_REFCOUNTED_TYPE(TInMemoryManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
