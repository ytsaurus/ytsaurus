#include "medium_aware_block_cache_manager.h"

#include "config.h"

#include <yt/yt/ytlib/chunk_client/client_block_cache.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <atomic>

namespace NYT::NDataNode {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

void ScaleCapacity(const TSlruCacheConfigPtr& config, int locationCount)
{
    YT_VERIFY(locationCount >= 0);
    config->Capacity *= locationCount;
}

void ScaleCapacity(
    const TSlruCacheConfigPtr& staticConfig,
    const TSlruCacheDynamicConfigPtr& dynamicConfig,
    int locationCount)
{
    YT_VERIFY(locationCount >= 0);
    dynamicConfig->Capacity = dynamicConfig->Capacity.value_or(staticConfig->Capacity) * locationCount;
}

TBlockCacheConfigPtr CreateEffectiveBlockCacheConfig(
    const TBlockCacheConfigPtr& config,
    int locationCount)
{
    auto result = NYTree::CloneYsonStruct(config);
    ScaleCapacity(result->CompressedData, locationCount);
    ScaleCapacity(result->UncompressedData, locationCount);
    ScaleCapacity(result->HashTableChunkIndex, locationCount);
    ScaleCapacity(result->XorFilter, locationCount);
    ScaleCapacity(result->ChunkFragmentsData, locationCount);
    ScaleCapacity(result->MinHashDigest, locationCount);
    return result;
}

TBlockCacheDynamicConfigPtr CreateEffectiveBlockCacheDynamicConfig(
    const TBlockCacheConfigPtr& staticConfig,
    const TBlockCacheDynamicConfigPtr& dynamicConfig,
    int locationCount)
{
    auto result = NYTree::CloneYsonStruct(dynamicConfig);
    ScaleCapacity(staticConfig->CompressedData, result->CompressedData, locationCount);
    ScaleCapacity(staticConfig->UncompressedData, result->UncompressedData, locationCount);
    ScaleCapacity(staticConfig->HashTableChunkIndex, result->HashTableChunkIndex, locationCount);
    ScaleCapacity(staticConfig->XorFilter, result->XorFilter, locationCount);
    ScaleCapacity(staticConfig->ChunkFragmentsData, result->ChunkFragmentsData, locationCount);
    ScaleCapacity(staticConfig->MinHashDigest, result->MinHashDigest, locationCount);
    return result;
}

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
        TLocationCountPerMedium locationCountPerMedium,
        IMemoryUsageTrackerPtr memoryUsageTracker,
        TMediumNameResolver mediumNameResolver,
        NProfiling::TProfiler profiler)
        : Config_(std::move(config))
        , LocationCountPerMedium_(std::move(locationCountPerMedium))
        , DynamicConfig_(New<TMediumAwareBlockCacheManagerDynamicConfig>())
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
            DynamicConfig_ = config;
            return;
        }

        auto oldCaches = GetPerMediumCaches();
        THashMap<std::string, IClientBlockCachePtr> caches;
        auto addCache = [&] (const std::string& mediumName) {
            if (caches.contains(mediumName)) {
                return;
            }

            auto oldCacheIt = oldCaches.find(mediumName);
            EmplaceOrCrash(
                caches,
                mediumName,
                oldCacheIt == oldCaches.end()
                    ? CreatePerMediumCache(mediumName)
                    : oldCacheIt->second);
        };
        for (const auto& [mediumName, _] : Config_->BlockCacheConfigPerMediumPerLocation) {
            addCache(mediumName);
        }
        for (const auto& [mediumName, _] : config->BlockCacheConfigPerMediumPerLocation) {
            addCache(mediumName);
        }

        THashMap<std::string, TBlockCacheDynamicConfigPtr> effectiveConfigs;
        for (const auto& [mediumName, _] : caches) {
            EmplaceOrCrash(
                effectiveConfigs,
                mediumName,
                CreateEffectiveDynamicConfig(config, mediumName, GetLocationCount(mediumName)));
        }

        for (const auto& [mediumName, cache] : caches) {
            cache->Reconfigure(GetOrCrash(effectiveConfigs, mediumName));
        }

        {
            auto guard = WriterGuard(PerMediumLock_);
            PerMedium_ = caches;
        }

        auto zeroCapacityConfig = CreateDisabledBlockCacheConfig();
        for (const auto& [mediumName, cache] : oldCaches) {
            if (!caches.contains(mediumName)) {
                cache->Reconfigure(zeroCapacityConfig);
            }
        }

        DynamicConfig_ = config;
        Enabled_.store(true, std::memory_order::relaxed);
    }

    void UpdateLocationCountPerMedium(const TLocationCountPerMedium& locationCountPerMedium) override
    {
        if (Enabled_.load(std::memory_order::relaxed)) {
            for (const auto& [mediumName, _] : GetPerMediumCaches()) {
                auto oldLocationCount = GetLocationCount(mediumName);
                auto newLocationCount = GetLocationCount(locationCountPerMedium, mediumName);
                if (newLocationCount != oldLocationCount) {
                    ReconfigureCache(mediumName, newLocationCount);
                }
            }
        }

        LocationCountPerMedium_ = locationCountPerMedium;
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
    TLocationCountPerMedium LocationCountPerMedium_;
    TMediumAwareBlockCacheManagerDynamicConfigPtr DynamicConfig_;
    const IMemoryUsageTrackerPtr MemoryUsageTracker_;
    const TMediumNameResolver MediumNameResolver_;
    const NProfiling::TProfiler Profiler_;

    std::atomic<bool> Enabled_ = false;

    mutable YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, PerMediumLock_);
    THashMap<std::string, IClientBlockCachePtr> PerMedium_;

    static int GetLocationCount(
        const TLocationCountPerMedium& locationCountPerMedium,
        const std::string& mediumName)
    {
        auto it = locationCountPerMedium.find(mediumName);
        return it == locationCountPerMedium.end() ? 0 : it->second;
    }

    int GetLocationCount(const std::string& mediumName) const
    {
        return GetLocationCount(LocationCountPerMedium_, mediumName);
    }

    TBlockCacheConfigPtr GetStaticConfig(const std::string& mediumName) const
    {
        auto it = Config_->BlockCacheConfigPerMediumPerLocation.find(mediumName);
        return it == Config_->BlockCacheConfigPerMediumPerLocation.end()
            ? New<TBlockCacheConfig>()
            : it->second;
    }

    TBlockCacheDynamicConfigPtr CreateEffectiveDynamicConfig(
        const TMediumAwareBlockCacheManagerDynamicConfigPtr& managerDynamicConfig,
        const std::string& mediumName,
        int locationCount) const
    {
        auto staticConfig = GetStaticConfig(mediumName);
        auto dynamicConfigIt = managerDynamicConfig->BlockCacheConfigPerMediumPerLocation.find(mediumName);
        auto dynamicConfig = dynamicConfigIt == managerDynamicConfig->BlockCacheConfigPerMediumPerLocation.end()
            ? New<TBlockCacheDynamicConfig>()
            : dynamicConfigIt->second;
        return CreateEffectiveBlockCacheDynamicConfig(
            staticConfig,
            dynamicConfig,
            locationCount);
    }

    IClientBlockCachePtr CreatePerMediumCache(const std::string& mediumName) const
    {
        auto staticConfig = GetStaticConfig(mediumName);
        auto effectiveConfig = CreateEffectiveBlockCacheConfig(staticConfig, GetLocationCount(mediumName));
        return CreateClientBlockCache(
            effectiveConfig,
            EBlockType::UncompressedData | EBlockType::CompressedData | EBlockType::HashTableChunkIndex |
                EBlockType::XorFilter | EBlockType::ChunkFragmentsData | EBlockType::MinHashDigest,
            MemoryUsageTracker_,
            Profiler_.WithTag("medium", mediumName),
            /*manageMemoryLimit*/ false);
    }

    void ReconfigureCache(const std::string& mediumName, int locationCount)
    {
        auto caches = GetPerMediumCaches();
        GetOrCrash(caches, mediumName)->Reconfigure(
            CreateEffectiveDynamicConfig(DynamicConfig_, mediumName, locationCount));
    }

    THashMap<std::string, IClientBlockCachePtr> CreatePerMediumCaches() const
    {
        THashMap<std::string, IClientBlockCachePtr> result;
        for (const auto& [mediumName, _] : Config_->BlockCacheConfigPerMediumPerLocation) {
            EmplaceOrCrash(result, mediumName, CreatePerMediumCache(mediumName));
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
    TLocationCountPerMedium locationCountPerMedium,
    IMemoryUsageTrackerPtr memoryUsageTracker,
    TMediumNameResolver mediumNameResolver,
    NProfiling::TProfiler profiler)
{
    return New<TMediumAwareBlockCacheManager>(
        std::move(config),
        std::move(locationCountPerMedium),
        std::move(memoryUsageTracker),
        std::move(mediumNameResolver),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
