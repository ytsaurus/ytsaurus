#include "block_cache.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TCachedBlock::TCachedBlock(TBlockId blockId, const TSharedRef& data)
    : TCacheValueBase<TBlockId, TCachedBlock, TBlockIdHash>(blockId)
    , Data(data)
{ }

////////////////////////////////////////////////////////////////////////////////

void TBlockCacheConfig::Read(const TJsonObject* jsonConfig)
{
    if (jsonConfig == NULL)
        return;
    NYT::TryRead(jsonConfig, L"Capacity", &Capacity);
}

////////////////////////////////////////////////////////////////////////////////

TBlockCache::TBlockCache(const TBlockCacheConfig& config)
    : TCapacityLimitedCache<TBlockId, TCachedBlock, TBlockIdHash>(config.Capacity)
{}

TCachedBlock::TPtr TBlockCache::Put(TBlockId blockId, TSharedRef data)
{
    TCachedBlock* value = new TCachedBlock(blockId, data);
    Put(value);
    return Get(blockId);
}

void TBlockCache::Put(TCachedBlock::TPtr cachedBlock)
{
    TInsertCookie cookie(cachedBlock->GetKey());
    if (BeginInsert(&cookie))
        EndInsert(~cachedBlock, &cookie);
}

TCachedBlock::TPtr TBlockCache::Get(TBlockId blockId)
{
    TAsyncResultPtr result = Lookup(blockId);
    TCachedBlock::TPtr block;
    if (result->TryGet(&block) && ~block != NULL)
        return block;
    else
        return NULL;

}

////////////////////////////////////////////////////////////////////////////////

}
