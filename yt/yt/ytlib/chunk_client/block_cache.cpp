#include "block_cache.h"

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/memory/leaky_ref_counted_singleton.h>

#include <util/digest/multi.h>

namespace NYT::NChunkClient {

using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

bool TBlockInfo::operator < (const TBlockInfo& other) const
{
    return std::tie(BlockIndex, BlockSize) < std::tie(other.BlockIndex, BlockSize);
}

TBlockInfo::operator size_t() const
{
    return MultiHash(BlockIndex, BlockSize);
}

bool TBlockInfo::operator == (const TBlockInfo& other) const
{
    return BlockIndex == other.BlockIndex && BlockSize == other.BlockSize;
}

////////////////////////////////////////////////////////////////////////////////

class TActiveCachedBlockCookie
    : public ICachedBlockCookie
{
public:
    bool IsActive() const override
    {
        return true;
    }

    TFuture<void> GetBlockFuture() const override
    {
        YT_ABORT();
    }

    TCachedBlock GetBlock() const override
    {
        YT_ABORT();
    }

    void SetBlock(TErrorOr<TCachedBlock> /*blockOrError*/) override
    { }
};

////////////////////////////////////////////////////////////////////////////////

class TPresetCachedBlockCookie
    : public ICachedBlockCookie
{
public:
    explicit TPresetCachedBlockCookie(TCachedBlock cachedBlock)
        : CachedBlock_(std::move(cachedBlock))
    { }

    bool IsActive() const override
    {
        return false;
    }

    TFuture<void> GetBlockFuture() const override
    {
        return VoidFuture;
    }

    TCachedBlock GetBlock() const override
    {
        return CachedBlock_;
    }

    void SetBlock(TErrorOr<TCachedBlock> /*blockOrError*/) override
    {
        YT_ABORT();
    }

private:
    const TCachedBlock CachedBlock_;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<ICachedBlockCookie> CreateActiveCachedBlockCookie()
{
    return std::make_unique<TActiveCachedBlockCookie>();
}

std::unique_ptr<ICachedBlockCookie> CreatePresetCachedBlockCookie(TCachedBlock cachedBlock)
{
    return std::make_unique<TPresetCachedBlockCookie>(std::move(cachedBlock));
}

////////////////////////////////////////////////////////////////////////////////

class TNullBlockCache
    : public IBlockCache
{
public:
    void PutBlock(
        const TBlockId& /*id*/,
        EBlockType /*type*/,
        const TBlock& /*data*/) override
    { }

    TCachedBlock FindBlock(
        const TBlockId& /*id*/,
        EBlockType /*type*/) override
    {
        return {};
    }

    std::unique_ptr<ICachedBlockCookie> GetBlockCookie(
        const TBlockId& /*id*/,
        EBlockType /*type*/) override
    {
        return CreateActiveCachedBlockCookie();
    }

    EBlockType GetSupportedBlockTypes() const override
    {
        return EBlockType::None;
    }

    void RemoveChunkBlocks(const TChunkId& /*chunkId*/) override
    { }

    THashSet<TBlockInfo> GetCachedBlocksByChunkId(TChunkId /*chunkId*/, EBlockType /*type*/) override
    {
        return {};
    }

    bool IsBlockTypeActive(EBlockType /*blockType*/) const override
    {
        return false;
    }
};

IBlockCachePtr GetNullBlockCache()
{
    return LeakyRefCountedSingleton<TNullBlockCache>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
