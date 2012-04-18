#include "stdafx.h"
#include "private.h"
#include "config.h"
#include "client_block_cache.h"

#include <ytlib/chunk_server/block_id.h>
#include <ytlib/misc/cache.h>
#include <ytlib/misc/property.h>

namespace NYT {
namespace NChunkClient {

using namespace NChunkServer;

///////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkClientLogger;

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
    : public TWeightLimitedCache<TBlockId, TCachedBlock>
    , public IBlockCache
{
public:
    typedef TClientBlockCacheConfig TConfig;

    TClientBlockCache(TConfig* config)
        : TWeightLimitedCache<TBlockId, TCachedBlock>(config->MaxSize)
    { }

    void Put(const TBlockId& id, const TSharedRef& data, const Stroka& source)
    {
        UNUSED(source);

        TInsertCookie cookie(id);
        if (BeginInsert(&cookie)) {
            auto block = New<TCachedBlock>(id, data);
            cookie.EndInsert(block);

            LOG_DEBUG("Block is put into cache (BlockId: %s, BlockSize: %" PRISZT ")",
                ~id.ToString(),
                data.Size());
        } else {
            // Already have the block cached, do nothing.
            LOG_DEBUG("Block is already in cache (BlockId: %s)", ~id.ToString());
        }
    }

    TSharedRef Find(const TBlockId& id)
    {
        auto asyncResult = Lookup(id);
        if (!asyncResult.IsNull()) {
            auto result = asyncResult.Get();
            YASSERT(result.IsOK());
            auto block = result.Value();

            LOG_DEBUG("Block cache hit (BlockId: %s)", ~id.ToString());

            return block->GetData();
        } else {
            LOG_DEBUG("Block cache miss (BlockId: %s)", ~id.ToString());
            return TSharedRef();
        }
    }

private:
    virtual i64 GetWeight(TCachedBlock* block) const
    {
        return block->GetData().Size();
    }
};

IBlockCachePtr CreateClientBlockCache(TClientBlockCacheConfig* config)
{
    YASSERT(config);
    return New<TClientBlockCache>(config);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

