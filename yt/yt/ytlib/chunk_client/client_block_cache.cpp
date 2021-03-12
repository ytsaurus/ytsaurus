#include "client_block_cache.h"
#include "private.h"
#include "block_cache.h"
#include "config.h"
#include "block_id.h"

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/async_slru_cache.h>
#include <yt/yt/core/misc/config.h>
#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/singleton.h>
#include <yt/yt/core/misc/sync_cache.h>

namespace NYT::NChunkClient {

using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkClientLogger;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TAsyncBlockCacheEntry)

class TAsyncBlockCacheEntry
    : public TAsyncCacheValueBase<TBlockId, TAsyncBlockCacheEntry>
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TCachedBlock, CachedBlock);

public:
    TAsyncBlockCacheEntry(TBlockId id, TCachedBlock cachedBlock)
        : TAsyncCacheValueBase(id)
        , CachedBlock_(std::move(cachedBlock))
    { }
};

DEFINE_REFCOUNTED_TYPE(TAsyncBlockCacheEntry)

////////////////////////////////////////////////////////////////////////////////

class TCachedBlockCookie
    : public ICachedBlockCookie
{
public:
    using TAsyncCacheCookie = TAsyncSlruCacheBase<TBlockId, TAsyncBlockCacheEntry>::TInsertCookie;

public:
    explicit TCachedBlockCookie(TAsyncCacheCookie cookie)
        : Cookie_(std::move(cookie))
    { }

    virtual bool IsActive() const
    {
        return Cookie_.IsActive();
    }

    virtual TFuture<TCachedBlock> GetBlockFuture() const override
    {
        return Cookie_.GetValue().Apply(BIND([] (const TErrorOr<TIntrusivePtr<TAsyncBlockCacheEntry>>& entryOrError) -> TErrorOr<TCachedBlock> {
            if (entryOrError.IsOK()) {
                return entryOrError.Value()->GetCachedBlock();
            } else {
                return static_cast<TError>(entryOrError);
            }
        }));
    }

    virtual void SetBlock(TErrorOr<TCachedBlock> blockOrError) override
    {
        if (blockOrError.IsOK()) {
            const auto& block = blockOrError.Value();
            auto entry = New<TAsyncBlockCacheEntry>(Cookie_.GetKey(), block);
            Cookie_.EndInsert(std::move(entry));
        } else {
            Cookie_.Cancel(static_cast<TError>(blockOrError));
        }
    }

private:
    TAsyncCacheCookie Cookie_;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TPerTypeClientBlockCache)

class TPerTypeClientBlockCache
    : public TMemoryTrackingAsyncSlruCacheBase<TBlockId, TAsyncBlockCacheEntry>
{
public:
    TPerTypeClientBlockCache(
        EBlockType type,
        TSlruCacheConfigPtr config,
        IMemoryUsageTrackerPtr memoryTracker,
        const NProfiling::TProfiler& profiler)
        : TMemoryTrackingAsyncSlruCacheBase(
            std::move(config),
            std::move(memoryTracker),
            profiler)
        , Type_(type)
    { }

    void PutBlock(const TBlockId& id, const TBlock& block)
    {
        if (Capacity_.load() == 0) {
            // Shortcut when cache is disabled.
            return;
        }

        auto cookie = BeginInsert(id);
        if (cookie.IsActive()) {
            auto entry = New<TAsyncBlockCacheEntry>(id, TCachedBlock(block));
            cookie.EndInsert(std::move(entry));

            YT_LOG_DEBUG("Block is put into cache (BlockId: %v, BlockType: %v, BlockSize: %v)",
                id,
                Type_,
                block.Size());
        } else {
            // Already have the block cached, do nothing.
            YT_LOG_TRACE("Block is already in cache (BlockId: %v, BlockType: %v)",
                id,
                Type_);
        }
    }

    TCachedBlock FindBlock(const TBlockId& id)
    {
        if (Capacity_.load() == 0) {
            // Shortcut when cache is disabled.
            return {};
        }

        auto block = TMemoryTrackingAsyncSlruCacheBase::Find(id);
        if (block) {
            YT_LOG_TRACE("Block cache hit (BlockId: %v, BlockType: %v)",
                id,
                Type_);
            return block->GetCachedBlock();
        } else {
            YT_LOG_TRACE("Block cache miss (BlockId: %v, BlockType: %v)",
                id,
                Type_);
            return {};
        }
    }

    std::unique_ptr<ICachedBlockCookie> GetBlockCookie(
        const TBlockId& id,
        EBlockType type)
    {
        YT_VERIFY(type == Type_);

        if (Capacity_.load() == 0) {
            // Shortcut when cache is disabled.
            return CreateActiveCachedBlockCookie();
        }

        auto cookie = BeginInsert(id);
        return std::make_unique<TCachedBlockCookie>(std::move(cookie));
    }

    std::vector<TBlockCacheEntry> GetSnapshot()
    {
        auto entries = GetAll();

        std::vector<TBlockCacheEntry> snapshot;
        snapshot.reserve(entries.size());
        for (const auto& entry : entries) {
            snapshot.push_back(TBlockCacheEntry{
                .BlockId = entry->GetKey(),
                .BlockType = Type_,
                .Block = entry->GetCachedBlock()
            });
        }

        return snapshot;
    }

private:
    const EBlockType Type_;

    virtual i64 GetWeight(const TAsyncBlockCacheEntryPtr& entry) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return entry->GetCachedBlock().Block.Size();
    }
};

DEFINE_REFCOUNTED_TYPE(TPerTypeClientBlockCache)

////////////////////////////////////////////////////////////////////////////////

class TClientBlockCache
    : public IClientBlockCache
{
public:
    TClientBlockCache(
        TBlockCacheConfigPtr config,
        EBlockType supportedBlockTypes,
        IMemoryUsageTrackerPtr memoryTracker,
        const NProfiling::TProfiler& profiler)
        : SupportedBlockTypes_(supportedBlockTypes)
    {
        auto initType = [&] (EBlockType type, TSlruCacheConfigPtr config) {
            if (Any(SupportedBlockTypes_ & type)) {
                auto cache = New<TPerTypeClientBlockCache>(
                    type,
                    config,
                    memoryTracker,
                    profiler.WithPrefix("/" + FormatEnum(type)));
                YT_VERIFY(PerTypeCaches_.emplace(type, cache).second);
            }
        };
        initType(EBlockType::CompressedData, config->CompressedData);
        initType(EBlockType::UncompressedData, config->UncompressedData);
    }

    virtual void PutBlock(
        const TBlockId& id,
        EBlockType type,
        const TBlock& data,
        const std::optional<TNodeDescriptor>& /*source*/) override
    {
        if (const auto& cache = FindPerTypeCache(type)) {
            cache->PutBlock(id, data);
        }
    }

    virtual TCachedBlock FindBlock(
        const TBlockId& id,
        EBlockType type) override
    {
        const auto& cache = FindPerTypeCache(type);
        return cache ? cache->FindBlock(id) : TCachedBlock();
    }

    virtual std::unique_ptr<ICachedBlockCookie> GetBlockCookie(
        const TBlockId& id,
        EBlockType type) override
    {
        const auto& cache = FindPerTypeCache(type);
        return cache
            ? cache->GetBlockCookie(id, type)
            : CreateActiveCachedBlockCookie();
    }

    virtual EBlockType GetSupportedBlockTypes() const override
    {
        return SupportedBlockTypes_;
    }

    virtual std::vector<TBlockCacheEntry> GetSnapshot(EBlockType blockTypes) const override
    {
        std::vector<TBlockCacheEntry> snapshot;
        for (const auto& [blockType, cache] : PerTypeCaches_) {
            if (Any(blockType & blockTypes)) {
                auto cacheSnapshot = cache->GetSnapshot();
                snapshot.insert(snapshot.end(), cacheSnapshot.begin(), cacheSnapshot.end());
            }
        }

        return snapshot;
    }

    virtual void Reconfigure(const TBlockCacheDynamicConfigPtr& config) override
    {
        auto reconfigureType = [&] (EBlockType type, TSlruCacheDynamicConfigPtr config) {
            if (const auto& cache = FindPerTypeCache(type)) {
                cache->Reconfigure(config);
            }
        };
        reconfigureType(EBlockType::CompressedData, config->CompressedData);
        reconfigureType(EBlockType::UncompressedData, config->UncompressedData);
    }

private:
    const EBlockType SupportedBlockTypes_;

    THashMap<EBlockType, TPerTypeClientBlockCachePtr> PerTypeCaches_;

    const TPerTypeClientBlockCachePtr& FindPerTypeCache(EBlockType type)
    {
        auto it = PerTypeCaches_.find(type);
        static TPerTypeClientBlockCachePtr NullCache;
        return it == PerTypeCaches_.end() ? NullCache : it->second;
    }
};

IClientBlockCachePtr CreateClientBlockCache(
    TBlockCacheConfigPtr config,
    EBlockType supportedBlockTypes,
    IMemoryUsageTrackerPtr memoryTracker,
    const NProfiling::TProfiler& profiler)
{
    return New<TClientBlockCache>(
        std::move(config),
        supportedBlockTypes,
        std::move(memoryTracker),
        profiler);
}

////////////////////////////////////////////////////////////////////////////////

class TNullBlockCache
    : public IBlockCache
{
public:
    virtual void PutBlock(
        const TBlockId& /* id */,
        EBlockType /* type */,
        const TBlock& /* data */,
        const std::optional<TNodeDescriptor>& /* source */) override
    { }

    virtual TCachedBlock FindBlock(
        const TBlockId& /* id */,
        EBlockType /* type */) override
    {
        return TCachedBlock();
    }

    virtual std::unique_ptr<ICachedBlockCookie> GetBlockCookie(
        const TBlockId& /* id */,
        EBlockType /* type */)
    {
        return CreateActiveCachedBlockCookie();
    }

    virtual EBlockType GetSupportedBlockTypes() const override
    {
        return EBlockType::None;
    }
};

IBlockCachePtr GetNullBlockCache()
{
    return RefCountedSingleton<TNullBlockCache>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

