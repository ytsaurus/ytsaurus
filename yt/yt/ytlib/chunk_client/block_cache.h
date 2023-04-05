#pragma once

#include "block.h"
#include "block_id.h"

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

using TCachedBlock = TBlock;

////////////////////////////////////////////////////////////////////////////////

struct ICachedBlockCookie
{
    virtual ~ICachedBlockCookie() = default;

    //! If |true|, block should be fetched and put into the cache via #SetBlock call.
    //! If |false|, block should be waited for on #GetBlockFuture call and then captured via #GetBlock call.
    virtual bool IsActive() const = 0;

    virtual TFuture<void> GetBlockFuture() const = 0;

    virtual TCachedBlock GetBlock() const = 0;

    virtual void SetBlock(TErrorOr<TCachedBlock> blockOrError) = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ICachedBlockCookie> CreateActiveCachedBlockCookie();

std::unique_ptr<ICachedBlockCookie> CreatePresetCachedBlockCookie(TCachedBlock cachedBlock);

////////////////////////////////////////////////////////////////////////////////

//! A simple asynchronous interface for caching chunk blocks.
/*!
 *  \note
 *  Thread affinity: any
 */
struct IBlockCache
    : public virtual TRefCounted
{
    //! Puts a block into the cache.
    /*!
     *  If a block with the given id is already present, then the request is ignored.
     */
    virtual void PutBlock(
        const TBlockId& id,
        EBlockType type,
        const TBlock& data) = 0;

    //! Fetches a block from the cache.
    /*!
     *  If no such block is present, null block is returned.
     */
    virtual TCachedBlock FindBlock(
        const TBlockId& id,
        EBlockType type) = 0;

    //! Returns a cookie for working with block in cache.
    virtual std::unique_ptr<ICachedBlockCookie> GetBlockCookie(
        const TBlockId& id,
        EBlockType type) = 0;

    //! Returns the set of supported block types.
    virtual EBlockType GetSupportedBlockTypes() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IBlockCache)

////////////////////////////////////////////////////////////////////////////////

//! Returns an always-empty block cache.
IBlockCachePtr GetNullBlockCache();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
