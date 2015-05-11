#pragma once

#include "public.h"

#include <core/misc/ref.h>

#include <ytlib/chunk_client/public.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

//! Contains chunk data (e.g. blocks) intercepted during write-out.
struct TInterceptedChunkData
    : public TIntrinsicRefCounted
{
    std::vector<TSharedRef> Blocks;
    EInMemoryMode InMemoryMode = EInMemoryMode::None;
};

DEFINE_REFCOUNTED_TYPE(TInterceptedChunkData)

////////////////////////////////////////////////////////////////////////////////

//! Manages in-memory tables served by the node.
/*!
 *  Ensures that chunk stores of in-memory tables are preloaded when a node starts.
 *
 *  Provides means for intercepting data write-out during flushes and compactions
 *  and thus enable new chunk stores to be created with all blocks already resident.
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
    TInterceptedChunkDataPtr EvictInterceptedChunkData(const NChunkClient::TChunkId& chunkId);

private:
    class TImpl;
    using TImplPtr = TIntrusivePtr<TImpl>;
    const TImplPtr Impl_;

};

DEFINE_REFCOUNTED_TYPE(TInMemoryManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
