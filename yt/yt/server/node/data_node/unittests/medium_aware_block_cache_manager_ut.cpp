#include <gtest/gtest.h>

#include <yt/yt/server/node/data_node/medium_aware_block_cache_manager.h>

#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/core/test_framework/test_memory_tracker.h>

#include <yt/yt/core/misc/cache_config.h>

namespace NYT::NDataNode {
namespace {

using namespace NChunkClient;

TBlockCacheConfigPtr CreateBlockCacheConfig(
    i64 compressedDataCapacity,
    i64 uncompressedDataCapacity = 0)
{
    auto config = New<TBlockCacheConfig>();
    config->CompressedData = TSlruCacheConfig::CreateWithCapacity(compressedDataCapacity, /*shardCount*/ 1);
    config->UncompressedData = TSlruCacheConfig::CreateWithCapacity(uncompressedDataCapacity, /*shardCount*/ 1);
    return config;
}

IMediumAwareBlockCacheManagerPtr CreateManager(
    const TMediumAwareBlockCacheManagerConfigPtr& managerConfig,
    const IMemoryUsageTrackerPtr& tracker)
{
    return CreateMediumAwareBlockCacheManager(
        managerConfig,
        tracker,
        BIND([] (int mediumIndex) -> std::optional<std::string> {
            if (mediumIndex == 42) {
                return "ssd_blobs";
            }
            if (mediumIndex == 43) {
                return "ssd_intermediate";
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
        CreateBlockCacheConfig(/*compressedDataCapacity*/ 1000),
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

TEST(TMediumAwareBlockCacheManagerTest, RoutesConfiguredMediumToDedicatedCache)
{
    auto tracker = New<TTestNodeMemoryTracker>(1_GB);

    auto managerConfig = New<TMediumAwareBlockCacheManagerConfig>();
    managerConfig->Enable = true;
    managerConfig->BlockCacheConfigPerMedium["ssd_blobs"] =
        CreateBlockCacheConfig(/*compressedDataCapacity*/ 100);
    auto manager = CreateManager(managerConfig, tracker);

    auto mediumCache = manager->GetBlockCacheForMedium(/*mediumIndex*/ 42);
    ASSERT_TRUE(mediumCache);
    EXPECT_FALSE(manager->GetBlockCacheForMedium(/*mediumIndex*/ 43));
    EXPECT_FALSE(manager->GetBlockCacheForMedium(/*mediumIndex*/ 44));

    auto chunkId = TChunkId::Create();
    TBlockId blockId(chunkId, /*blockIndex*/ 0);
    mediumCache->PutBlock(
        blockId,
        EBlockType::CompressedData,
        TBlock(TSharedRef::FromString(TString("data"))));

    EXPECT_TRUE(mediumCache->FindBlock(blockId, EBlockType::CompressedData));
}

TEST(TMediumAwareBlockCacheManagerTest, ReconfiguresPerMediumCache)
{
    auto tracker = New<TTestNodeMemoryTracker>(1_GB);

    auto managerConfig = New<TMediumAwareBlockCacheManagerConfig>();
    managerConfig->Enable = true;
    managerConfig->BlockCacheConfigPerMedium["ssd_blobs"] =
        CreateBlockCacheConfig(/*compressedDataCapacity*/ 100);
    auto manager = CreateManager(managerConfig, tracker);

    auto mediumCache = manager->GetBlockCacheForMedium(/*mediumIndex*/ 42);
    ASSERT_TRUE(mediumCache);
    ASSERT_TRUE(mediumCache->IsBlockTypeActive(EBlockType::CompressedData));

    auto managerDynamicConfig = New<TMediumAwareBlockCacheManagerDynamicConfig>();
    auto mediumDynamicConfig = New<TBlockCacheDynamicConfig>();
    mediumDynamicConfig->CompressedData->Capacity = 0;
    managerDynamicConfig->BlockCacheConfigPerMedium["ssd_blobs"] = mediumDynamicConfig;
    manager->Reconfigure(managerDynamicConfig);

    EXPECT_FALSE(mediumCache->IsBlockTypeActive(EBlockType::CompressedData));
}

TEST(TMediumAwareBlockCacheManagerTest, ClearsPerMediumCachesWhenDisabled)
{
    auto tracker = New<TTestNodeMemoryTracker>(1_GB);

    auto managerConfig = New<TMediumAwareBlockCacheManagerConfig>();
    managerConfig->Enable = true;
    managerConfig->BlockCacheConfigPerMedium["ssd_blobs"] =
        CreateBlockCacheConfig(/*compressedDataCapacity*/ 100);
    auto manager = CreateManager(managerConfig, tracker);

    auto mediumCache = manager->GetBlockCacheForMedium(/*mediumIndex*/ 42);
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
    EXPECT_FALSE(manager->GetBlockCacheForMedium(/*mediumIndex*/ 42));
    EXPECT_FALSE(mediumCache->IsBlockTypeActive(EBlockType::CompressedData));
    EXPECT_FALSE(mediumCache->FindBlock(blockId, EBlockType::CompressedData));
    EXPECT_EQ(tracker->GetUsed(), 0);

    auto disabledMediumCache = mediumCache;
    auto enabledConfig = New<TMediumAwareBlockCacheManagerDynamicConfig>();
    enabledConfig->Enable = true;
    manager->Reconfigure(enabledConfig);
    mediumCache = manager->GetBlockCacheForMedium(/*mediumIndex*/ 42);
    ASSERT_TRUE(mediumCache);
    EXPECT_NE(mediumCache.Get(), disabledMediumCache.Get());
    EXPECT_FALSE(mediumCache->FindBlock(blockId, EBlockType::CompressedData));
}

TEST(TMediumAwareBlockCacheManagerTest, AggregatesAndRemovesBlocksAcrossPerMediumCaches)
{
    auto tracker = New<TTestNodeMemoryTracker>(1_GB);

    auto managerConfig = New<TMediumAwareBlockCacheManagerConfig>();
    managerConfig->Enable = true;
    managerConfig->BlockCacheConfigPerMedium["ssd_blobs"] =
        CreateBlockCacheConfig(/*compressedDataCapacity*/ 100);
    managerConfig->BlockCacheConfigPerMedium["ssd_intermediate"] =
        CreateBlockCacheConfig(/*compressedDataCapacity*/ 100);
    auto manager = CreateManager(managerConfig, tracker);
    auto firstMediumCache = manager->GetBlockCacheForMedium(/*mediumIndex*/ 42);
    auto secondMediumCache = manager->GetBlockCacheForMedium(/*mediumIndex*/ 43);
    ASSERT_TRUE(firstMediumCache);
    ASSERT_TRUE(secondMediumCache);

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
