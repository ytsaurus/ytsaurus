#include "block_cache.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/singleton.h>

namespace NYT::NChunkClient {

using namespace NNodeTrackerClient;

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
};

IBlockCachePtr GetNullBlockCache()
{
    return LeakyRefCountedSingleton<TNullBlockCache>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
