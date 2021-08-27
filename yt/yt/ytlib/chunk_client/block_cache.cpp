#include "block_cache.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NChunkClient {

using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

TCachedBlock::TCachedBlock(TBlock block, bool p2p)
    : Block(std::move(block))
    , P2P(p2p)
{ }

////////////////////////////////////////////////////////////////////////////////

class TActiveCachedBlockCookie
    : public ICachedBlockCookie
{
public:
    bool IsActive() const override
    {
        return true;
    }

    TFuture<TCachedBlock> GetBlockFuture() const override
    {
        YT_ABORT();
    }

    void SetBlock(TErrorOr<TCachedBlock> /* blockOrError */) override
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

    TFuture<TCachedBlock> GetBlockFuture() const override
    {
        return MakeFuture<TCachedBlock>(CachedBlock_);
    }

    void SetBlock(TErrorOr<TCachedBlock> /* blockOrError */) override
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

} // namespace NYT::NChunkClient
