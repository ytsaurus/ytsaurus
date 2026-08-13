#include <gtest/gtest.h>

#include <yt/yt/server/node/data_node/medium_aware_block_cache_manager.h>

#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/core/test_framework/test_memory_tracker.h>

#include <yt/yt/core/misc/cache_config.h>

namespace NYT::NDataNode {
namespace {

using namespace NChunkClient;

const std::string SsdBlobsMediumName = "ssd_blobs";
const std::string SsdIntermediateMediumName = "ssd_intermediate";

constexpr int SsdBlobsMediumIndex = 42;
constexpr int SsdIntermediateMediumIndex = 43;
constexpr int UnknownMediumIndex = 44;

TBlockCacheConfigPtr CreateOrdinaryBlockCacheConfig(
    i64 compressedDataCapacity,
    i64 uncompressedDataCapacity = 0)
{
    auto config = New<TBlockCacheConfig>();
    config->CompressedData = TSlruCacheConfig::CreateWithCapacity(compressedDataCapacity, /*shardCount*/ 1);
    config->UncompressedData = TSlruCacheConfig::CreateWithCapacity(uncompressedDataCapacity, /*shardCount*/ 1);
    return config;
}

TBlockCacheConfigPtr CreateMediumAwareBlockCacheConfig(i64 compressedDataCapacityPerLocation)
{
    auto config = New<TBlockCacheConfig>();
    config->CompressedData->Capacity = compressedDataCapacityPerLocation;
    return config;
}

TBlockCacheDynamicConfigPtr CreateMediumAwareBlockCacheDynamicConfig(
    i64 compressedDataCapacityPerLocation,
    std::optional<i64> uncompressedDataCapacityPerLocation = {})
{
    auto config = New<TBlockCacheDynamicConfig>();
    config->CompressedData->Capacity = compressedDataCapacityPerLocation;
    config->UncompressedData->Capacity = uncompressedDataCapacityPerLocation;
    return config;
}

TMediumAwareBlockCacheManagerConfigPtr CreateManagerConfig(
    const THashMap<std::string, i64>& compressedDataCapacityPerMediumPerLocation)
{
    auto config = New<TMediumAwareBlockCacheManagerConfig>();
    config->Enable = true;
    for (const auto& [mediumName, capacity] : compressedDataCapacityPerMediumPerLocation) {
        config->BlockCacheConfigPerMediumPerLocation[mediumName] =
            CreateMediumAwareBlockCacheConfig(capacity);
    }
    return config;
}

IMediumAwareBlockCacheManagerPtr CreateManager(
    const TMediumAwareBlockCacheManagerConfigPtr& managerConfig,
    const IMemoryUsageTrackerPtr& tracker,
    TLocationCountPerMedium locationCountPerMedium = {{SsdBlobsMediumName, 1}})
{
    return CreateMediumAwareBlockCacheManager(
        managerConfig,
        std::move(locationCountPerMedium),
        tracker,
        BIND([] (int mediumIndex) -> std::optional<std::string> {
            if (mediumIndex == SsdBlobsMediumIndex) {
                return SsdBlobsMediumName;
            }
            if (mediumIndex == SsdIntermediateMediumIndex) {
                return SsdIntermediateMediumName;
            }
            return std::nullopt;
        }),
        {});
}

////////////////////////////////////////////////////////////////////////////////

TEST(TClientBlockCacheMemoryLimitTest, DoesNotUpdateTrackerLimitWhenMemoryLimitManagementIsDisabled)
{
    auto tracker = New<TTestNodeMemoryTracker>(1_GB);
    auto cache = CreateClientBlockCache(
        CreateOrdinaryBlockCacheConfig(/*compressedDataCapacity*/ 1000),
        EBlockType::CompressedData,
        tracker,
        /*profiler*/ {},
        /*manageMemoryLimit*/ false);

    EXPECT_EQ(tracker->GetLimit(), 1_GB);

    auto dynamicConfig = New<TBlockCacheDynamicConfig>();
    dynamicConfig->CompressedData->Capacity = 2000;
    cache->Reconfigure(dynamicConfig);

    EXPECT_EQ(tracker->GetLimit(), 1_GB);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TMediumAwareBlockCacheManagerTest, ScalesStaticAndDynamicCapacityWithLocationCount)
{
    auto tracker = New<TTestNodeMemoryTracker>(1_GB);

    auto managerConfig = CreateManagerConfig({{SsdBlobsMediumName, 4}});
    auto blockCacheConfig = GetOrCrash(
        managerConfig->BlockCacheConfigPerMediumPerLocation,
        SsdBlobsMediumName);
    blockCacheConfig->CompressedData->ShardCount = 1;
    auto manager = CreateManager(managerConfig, tracker, {});

    auto mediumCache = manager->GetBlockCacheForMedium(SsdBlobsMediumIndex);
    ASSERT_TRUE(mediumCache);
    EXPECT_FALSE(mediumCache->IsBlockTypeActive(EBlockType::CompressedData));

    manager->UpdateLocationCountPerMedium({{SsdBlobsMediumName, 1}});
    EXPECT_TRUE(mediumCache->IsBlockTypeActive(EBlockType::CompressedData));

    auto chunkId = TChunkId::Create();
    std::vector<TBlockId> blockIds = {
        TBlockId(chunkId, /*blockIndex*/ 0),
        TBlockId(chunkId, /*blockIndex*/ 1),
    };

    auto putBlocks = [&] {
        for (const auto& blockId : blockIds) {
            mediumCache->PutBlock(
                blockId,
                EBlockType::CompressedData,
                TBlock(TSharedRef::FromString(TString("data"))));
        }
    };

    putBlocks();
    EXPECT_EQ(mediumCache->GetCachedBlocksByChunkId(chunkId, EBlockType::CompressedData).size(), 1u);

    manager->UpdateLocationCountPerMedium({{SsdBlobsMediumName, 2}});
    putBlocks();
    EXPECT_EQ(mediumCache->GetCachedBlocksByChunkId(chunkId, EBlockType::CompressedData).size(), 2u);

    manager->UpdateLocationCountPerMedium({{SsdBlobsMediumName, 1}});
    EXPECT_EQ(mediumCache->GetCachedBlocksByChunkId(chunkId, EBlockType::CompressedData).size(), 1u);

    manager->UpdateLocationCountPerMedium({});
    EXPECT_FALSE(mediumCache->IsBlockTypeActive(EBlockType::CompressedData));
    EXPECT_TRUE(mediumCache->GetCachedBlocksByChunkId(chunkId, EBlockType::CompressedData).empty());

    manager->UpdateLocationCountPerMedium({{SsdBlobsMediumName, 2}});
    putBlocks();
    EXPECT_EQ(mediumCache->GetCachedBlocksByChunkId(chunkId, EBlockType::CompressedData).size(), 2u);

    // Two dynamic bytes per location and two locations give a four-byte cache that holds one block.
    auto dynamicConfig = New<TMediumAwareBlockCacheManagerDynamicConfig>();
    dynamicConfig->BlockCacheConfigPerMediumPerLocation[SsdBlobsMediumName] =
        CreateMediumAwareBlockCacheDynamicConfig(/*compressedDataCapacityPerLocation*/ 2);
    manager->Reconfigure(dynamicConfig);
    EXPECT_EQ(mediumCache->GetCachedBlocksByChunkId(chunkId, EBlockType::CompressedData).size(), 1u);
}

TEST(TMediumAwareBlockCacheManagerTest, ClearsPerMediumCachesWhenDisabled)
{
    auto tracker = New<TTestNodeMemoryTracker>(1_GB);

    auto managerConfig = CreateManagerConfig({{SsdBlobsMediumName, 100}});
    auto manager = CreateManager(managerConfig, tracker);

    auto mediumCache = manager->GetBlockCacheForMedium(SsdBlobsMediumIndex);
    ASSERT_TRUE(mediumCache);
    auto blockId = TBlockId(TChunkId::Create(), /*blockIndex*/ 0);
    mediumCache->PutBlock(
        blockId,
        EBlockType::CompressedData,
        TBlock(TSharedRef::FromString(TString("data"))));
    ASSERT_TRUE(mediumCache->FindBlock(blockId, EBlockType::CompressedData));
    ASSERT_EQ(tracker->GetUsed(), 4);

    auto disabledConfig = New<TMediumAwareBlockCacheManagerDynamicConfig>();
    disabledConfig->Enable = false;
    manager->Reconfigure(disabledConfig);
    EXPECT_FALSE(manager->GetBlockCacheForMedium(SsdBlobsMediumIndex));
    EXPECT_FALSE(mediumCache->IsBlockTypeActive(EBlockType::CompressedData));
    EXPECT_FALSE(mediumCache->FindBlock(blockId, EBlockType::CompressedData));
    EXPECT_EQ(tracker->GetUsed(), 0);

    manager->UpdateLocationCountPerMedium({});

    auto disabledMediumCache = mediumCache;
    auto enabledConfig = New<TMediumAwareBlockCacheManagerDynamicConfig>();
    enabledConfig->Enable = true;
    manager->Reconfigure(enabledConfig);
    mediumCache = manager->GetBlockCacheForMedium(SsdBlobsMediumIndex);
    ASSERT_TRUE(mediumCache);
    EXPECT_NE(mediumCache.Get(), disabledMediumCache.Get());
    EXPECT_FALSE(mediumCache->FindBlock(blockId, EBlockType::CompressedData));
    EXPECT_FALSE(mediumCache->IsBlockTypeActive(EBlockType::CompressedData));

    manager->UpdateLocationCountPerMedium({{SsdBlobsMediumName, 1}});
    EXPECT_TRUE(mediumCache->IsBlockTypeActive(EBlockType::CompressedData));
}

TEST(TMediumAwareBlockCacheManagerTest, RoutesAndManagesBlocksAcrossPerMediumCaches)
{
    auto tracker = New<TTestNodeMemoryTracker>(1_GB);

    auto managerConfig = CreateManagerConfig({
        {SsdBlobsMediumName, 100},
        {SsdIntermediateMediumName, 100},
    });
    auto manager = CreateManager(
        managerConfig,
        tracker,
        {{SsdBlobsMediumName, 1}, {SsdIntermediateMediumName, 1}});
    auto firstMediumCache = manager->GetBlockCacheForMedium(SsdBlobsMediumIndex);
    auto secondMediumCache = manager->GetBlockCacheForMedium(SsdIntermediateMediumIndex);
    ASSERT_TRUE(firstMediumCache);
    ASSERT_TRUE(secondMediumCache);
    EXPECT_FALSE(manager->GetBlockCacheForMedium(UnknownMediumIndex));

    auto chunkId = TChunkId::Create();
    TBlockId firstBlockId(chunkId, /*blockIndex*/ 0);
    TBlockId secondBlockId(chunkId, /*blockIndex*/ 1);
    firstMediumCache->PutBlock(
        firstBlockId,
        EBlockType::CompressedData,
        TBlock(TSharedRef::FromString(TString("first"))));
    secondMediumCache->PutBlock(
        secondBlockId,
        EBlockType::CompressedData,
        TBlock(TSharedRef::FromString(TString("second"))));

    auto cachedBlocks = manager->GetCachedBlocksByChunkId(chunkId, EBlockType::CompressedData);
    EXPECT_EQ(cachedBlocks.size(), 2u);

    manager->RemoveChunkBlocks(chunkId);
    EXPECT_FALSE(firstMediumCache->FindBlock(firstBlockId, EBlockType::CompressedData));
    EXPECT_FALSE(secondMediumCache->FindBlock(secondBlockId, EBlockType::CompressedData));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NDataNode
