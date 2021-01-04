#include "client_block_cache.h"
#include "private.h"
#include "block_cache.h"
#include "config.h"
#include "block_id.h"

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/config.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/singleton.h>
#include <yt/core/misc/sync_cache.h>

namespace NYT::NChunkClient {

using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkClientLogger;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCachedBlock)

class TCachedBlock
    : public TSyncCacheValueBase<TBlockId, TCachedBlock>
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TBlock, Data);

public:
    TCachedBlock(const TBlockId& id, const TBlock& data)
        : TSyncCacheValueBase(id)
        , Data_(data)
    { }
};

DEFINE_REFCOUNTED_TYPE(TCachedBlock)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TPerTypeClientBlockCache)

class TPerTypeClientBlockCache
    : public TMemoryTrackingSyncSlruCacheBase<TBlockId, TCachedBlock>
{
public:
    TPerTypeClientBlockCache(
        EBlockType type,
        TSlruCacheConfigPtr config,
        IMemoryUsageTrackerPtr memoryTracker,
        const NProfiling::TRegistry& profiler)
        : TMemoryTrackingSyncSlruCacheBase(
            std::move(config),
            std::move(memoryTracker),
            profiler)
        , Type_(type)
    { }

    void Put(const TBlockId& id, const TBlock& data)
    {
        if (Capacity_.load() == 0) {
            // Shortcut when cache is disabled.
            return;
        }

        auto block = New<TCachedBlock>(id, data);
        if (TryInsert(block)) {
            YT_LOG_DEBUG("Block is put into cache (BlockId: %v, BlockType: %v, BlockSize: %v)",
                id,
                Type_,
                data.Size());
        } else {
            // Already have the block cached, do nothing.
            YT_LOG_TRACE("Block is already in cache (BlockId: %v, BlockType: %v)",
                id,
                Type_);
        }
    }

    TBlock Find(const TBlockId& id)
    {
        if (Capacity_.load() == 0) {
            // Shortcut when cache is disabled.
            return {};
        }

        auto block = TSyncSlruCacheBase::Find(id);
        if (block) {
            YT_LOG_TRACE("Block cache hit (BlockId: %v, BlockType: %v)",
                id,
                Type_);
            return block->GetData();
        } else {
            YT_LOG_TRACE("Block cache miss (BlockId: %v, BlockType: %v)",
                id,
                Type_);
            return {};
        }
    }

private:
    const EBlockType Type_;

    virtual i64 GetWeight(const TCachedBlockPtr& block) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return block->GetData().Size();
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
        const NProfiling::TRegistry& profiler)
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

    virtual void Put(
        const TBlockId& id,
        EBlockType type,
        const TBlock& data,
        const std::optional<TNodeDescriptor>& /*source*/) override
    {
        if (const auto& cache = FindPerTypeCache(type)) {
            cache->Put(id, data);
        }
    }

    virtual TBlock Find(
        const TBlockId& id,
        EBlockType type) override
    {
        const auto& cache = FindPerTypeCache(type);
        return cache ? cache->Find(id) : TBlock();
    }

    virtual EBlockType GetSupportedBlockTypes() const override
    {
        return SupportedBlockTypes_;
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
    const NProfiling::TRegistry& profiler)
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
    virtual void Put(
        const TBlockId& /*id*/,
        EBlockType /*type*/,
        const TBlock& /*data*/,
        const std::optional<TNodeDescriptor>& /*source*/) override
    { }

    virtual TBlock Find(
        const TBlockId& /*id*/,
        EBlockType /*type*/) override
    {
        return TBlock();
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

