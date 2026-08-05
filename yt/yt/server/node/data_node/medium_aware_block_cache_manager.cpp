#include "medium_aware_block_cache_manager.h"

#include "config.h"

#include <yt/yt/ytlib/chunk_client/client_block_cache.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <atomic>

namespace NYT::NDataNode {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

TBlockCacheDynamicConfigPtr CreateDisabledBlockCacheConfig()
{
    auto config = New<TBlockCacheDynamicConfig>();
    config->CompressedData->Capacity = 0;
    config->UncompressedData->Capacity = 0;
    config->HashTableChunkIndex->Capacity = 0;
    config->XorFilter->Capacity = 0;
    config->ChunkFragmentsData->Capacity = 0;
    config->MinHashDigest->Capacity = 0;
    return config;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TMediumAwareBlockCacheManager
    : public IMediumAwareBlockCacheManager
{
public:
    TMediumAwareBlockCacheManager(
        TMediumAwareBlockCacheManagerConfigPtr config,
        IMemoryUsageTrackerPtr memoryUsageTracker,
        TMediumNameResolver mediumNameResolver,
        NProfiling::TProfiler profiler)
        : Config_(std::move(config))
        , MemoryUsageTracker_(std::move(memoryUsageTracker))
        , MediumNameResolver_(std::move(mediumNameResolver))
        , Profiler_(std::move(profiler))
    {
        if (Config_->Enable) {
            PerMedium_ = CreatePerMediumCaches();
            Enabled_.store(true, std::memory_order::relaxed);
        }
    }

    IBlockCachePtr GetBlockCacheForMedium(int mediumIndex) const override
    {
        if (!Enabled_.load(std::memory_order::relaxed)) {
            return nullptr;
        }

        auto mediumName = MediumNameResolver_(mediumIndex);
        if (!mediumName) {
            return nullptr;
        }

        auto guard = ReaderGuard(PerMediumLock_);
        if (auto it = PerMedium_.find(*mediumName); it != PerMedium_.end()) {
            return it->second;
        }
        return nullptr;
    }

    void Reconfigure(const TMediumAwareBlockCacheManagerDynamicConfigPtr& config) override
    {
        auto enabled = config->Enable.value_or(Config_->Enable);

        for (const auto& [mediumName, _] : config->BlockCacheConfigPerMedium) {
            THROW_ERROR_EXCEPTION_UNLESS(
                Config_->BlockCacheConfigPerMedium.contains(mediumName),
                "Cannot dynamically configure block cache for unknown medium %Qv.",
                mediumName);
        }

        // NB: Mutations happen on the control thread, while reads happen on RPC threads.
        if (!enabled) {
            Enabled_.store(false, std::memory_order::relaxed);
            THashMap<std::string, IClientBlockCachePtr> oldCaches;
            {
                auto guard = WriterGuard(PerMediumLock_);
                oldCaches.swap(PerMedium_);
            }
            auto zeroCapacityConfig = CreateDisabledBlockCacheConfig();
            for (const auto& [_, cache] : oldCaches) {
                cache->Reconfigure(zeroCapacityConfig);
            }
            return;
        }

        auto caches = GetPerMediumCaches();
        bool cachesCreated = false;
        if (caches.empty()) {
            caches = CreatePerMediumCaches();
            cachesCreated = true;
        }

        for (const auto& [mediumName, cache] : caches) {
            auto dynamicConfigIt = config->BlockCacheConfigPerMedium.find(mediumName);
            cache->Reconfigure(
                dynamicConfigIt == config->BlockCacheConfigPerMedium.end()
                    ? New<TBlockCacheDynamicConfig>()
                    : dynamicConfigIt->second);
        }

        if (cachesCreated) {
            auto guard = WriterGuard(PerMediumLock_);
            PerMedium_ = std::move(caches);
        }

        Enabled_.store(true, std::memory_order::relaxed);
    }

    void RemoveChunkBlocks(TChunkId chunkId) override
    {
        for (const auto& [_, cache] : GetPerMediumCaches()) {
            cache->RemoveChunkBlocks(chunkId);
        }
    }

    THashSet<TBlockInfo> GetCachedBlocksByChunkId(TChunkId chunkId, EBlockType type) override
    {
        THashSet<TBlockInfo> result;
        for (const auto& [_, cache] : GetPerMediumCaches()) {
            auto blocks = cache->GetCachedBlocksByChunkId(chunkId, type);
            result.insert(blocks.begin(), blocks.end());
        }
        return result;
    }

private:
    const TMediumAwareBlockCacheManagerConfigPtr Config_;
    const IMemoryUsageTrackerPtr MemoryUsageTracker_;
    const TMediumNameResolver MediumNameResolver_;
    const NProfiling::TProfiler Profiler_;

    std::atomic<bool> Enabled_ = false;

    mutable YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, PerMediumLock_);
    THashMap<std::string, IClientBlockCachePtr> PerMedium_;

    THashMap<std::string, IClientBlockCachePtr> CreatePerMediumCaches() const
    {
        THashMap<std::string, IClientBlockCachePtr> result;
        for (const auto& [mediumName, blockCacheConfig] : Config_->BlockCacheConfigPerMedium) {
            EmplaceOrCrash(
                result,
                mediumName,
                CreateClientBlockCache(
                    blockCacheConfig,
                    EBlockType::UncompressedData | EBlockType::CompressedData | EBlockType::HashTableChunkIndex |
                        EBlockType::XorFilter | EBlockType::ChunkFragmentsData | EBlockType::MinHashDigest,
                    MemoryUsageTracker_,
                    Profiler_.WithTag("medium", mediumName),
                    /*manageMemoryLimit*/ false));
        }
        return result;
    }

    THashMap<std::string, IClientBlockCachePtr> GetPerMediumCaches() const
    {
        auto guard = ReaderGuard(PerMediumLock_);
        return PerMedium_;
    }
};

////////////////////////////////////////////////////////////////////////////////

IMediumAwareBlockCacheManagerPtr CreateMediumAwareBlockCacheManager(
    TMediumAwareBlockCacheManagerConfigPtr config,
    IMemoryUsageTrackerPtr memoryUsageTracker,
    TMediumNameResolver mediumNameResolver,
    NProfiling::TProfiler profiler)
{
    return New<TMediumAwareBlockCacheManager>(
        std::move(config),
        std::move(memoryUsageTracker),
        std::move(mediumNameResolver),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
