#include "stdafx.h"
#include "private.h"
#include "config.h"
#include "block_cache.h"
#include "client_block_cache.h"

#include <core/misc/cache.h>
#include <core/misc/property.h>
#include <core/misc/singleton.h>

#include <ytlib/chunk_client/block_id.h>

#include <ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NChunkClient {

using namespace NNodeTrackerClient;

///////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkClientLogger;

///////////////////////////////////////////////////////////////////////////////

class TCachedBlock
    : public TCacheValueBase<TBlockId, TCachedBlock>
{
    DEFINE_BYVAL_RO_PROPERTY(TSharedRef, Data);

public:
    TCachedBlock(const TBlockId& id, const TSharedRef& data)
        : TCacheValueBase<TBlockId, TCachedBlock>(id)
        , Data_(data)
    { }

};

class TClientBlockCache
    : public TSlruCacheBase<TBlockId, TCachedBlock>
    , public IBlockCache
{
public:
    explicit TClientBlockCache(TSlruCacheConfigPtr config)
        : TSlruCacheBase(config)
    { }

    virtual void Put(
        const TBlockId& id,
        const TSharedRef& data,
        const TNullable<TNodeDescriptor>& /*source*/) override
    {
        TInsertCookie cookie(id);
        if (BeginInsert(&cookie)) {
            auto block = New<TCachedBlock>(id, data);
            cookie.EndInsert(block);

            LOG_DEBUG("Block is put into cache (BlockId: %v, BlockSize: %v)",
                id,
                data.Size());
        } else {
            // Already have the block cached, do nothing.
            LOG_DEBUG("Block is already in cache (BlockId: %v)", id);
        }
    }

    virtual TSharedRef Find(const TBlockId& id) override
    {
        auto asyncResult = Lookup(id);
        if (asyncResult) {
            auto result = asyncResult.Get();
            YCHECK(result.IsOK());
            auto block = result.Value();

            LOG_DEBUG("Block cache hit (BlockId: %v)", id);

            return block->GetData();
        } else {
            LOG_DEBUG("Block cache miss (BlockId: %v)", id);
            return TSharedRef();
        }
    }

private:
    virtual i64 GetWeight(TCachedBlock* block) const override
    {
        return block->GetData().Size();
    }

};

IBlockCachePtr CreateClientBlockCache(TSlruCacheConfigPtr config)
{
    return New<TClientBlockCache>(config);
}

///////////////////////////////////////////////////////////////////////////////

class TNullBlockCache
    : public IBlockCache
{
public:
    virtual void Put(
        const TBlockId& /*id*/,
        const TSharedRef& /*data*/,
        const TNullable<TNodeDescriptor>& /*source*/) override
    { }

    virtual TSharedRef Find(const TBlockId& /*id*/) override
    {
        return TSharedRef();
    }
};

IBlockCachePtr GetNullBlockCache()
{
    return RefCountedSingleton<TNullBlockCache>();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

