#include "block_cache.h"

#include <yt/core/actions/future.h>

namespace NYT::NChunkClient {

using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

TCachedBlock::TCachedBlock(TBlock block, std::optional<TNodeDescriptor> source)
    : Block(std::move(block))
    , Source(std::move(source))
{ }

////////////////////////////////////////////////////////////////////////////////

class TActiveCachedBlockCookie
    : public ICachedBlockCookie
{
public:
    virtual bool IsActive() const override
    {
        return true;
    }

    virtual TFuture<TCachedBlock> GetBlockFuture() const override
    {
        YT_ABORT();
    }

    virtual void SetBlock(TErrorOr<TCachedBlock> /* blockOrError */) override
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

    virtual bool IsActive() const override
    {
        return false;
    }

    virtual TFuture<TCachedBlock> GetBlockFuture() const override
    {
        return MakeFuture<TCachedBlock>(CachedBlock_);
    }

    virtual void SetBlock(TErrorOr<TCachedBlock> /* blockOrError */) override
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
