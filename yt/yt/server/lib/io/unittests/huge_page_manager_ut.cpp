#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/test_framework/test_memory_tracker.h>

#include <yt/yt/server/lib/io/config.h>
#include <yt/yt/server/lib/io/huge_page_manager.h>

namespace NYT::NIO {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(THugePageManagerTest, ResizeReleasesCachedBlobs)
{
    auto tracker = New<TTestNodeMemoryTracker>(1_GB);
    auto config = New<THugePageManagerConfig>();
    config->Type = EHugeManagerType::Transparent;
    config->Enabled = true;
    config->PagesPerBlob = 2;
    auto manager = CreateHugePageManager(config, /*profiler*/ {}, tracker);
    auto pageSize = manager->GetHugePageSize();
    if (pageSize == 0) {
        GTEST_SKIP() << "System huge page size is unavailable";
    }

    auto cachedBlob = manager->ReserveHugePageBlob()
        .ValueOrThrow();
    cachedBlob.Reset();
    EXPECT_EQ(manager->GetUsedHugePageCount(), 2);
    EXPECT_EQ(tracker->GetUsed(), 2 * pageSize);

    auto dynamicConfig = New<THugePageManagerDynamicConfig>();
    dynamicConfig->PagesPerBlob = 3;
    manager->Reconfigure(dynamicConfig);
    EXPECT_EQ(manager->GetUsedHugePageCount(), 0);
    EXPECT_EQ(tracker->GetUsed(), 0);
}

TEST(THugePageManagerTest, DestroyPreservesOutstandingReferences)
{
    auto tracker = New<TTestNodeMemoryTracker>(1_GB);
    auto config = New<THugePageManagerConfig>();
    config->Type = EHugeManagerType::Transparent;
    config->Enabled = true;
    config->PagesPerBlob = 2;
    auto manager = CreateHugePageManager(config, /*profiler*/ {}, tracker);
    auto pageSize = manager->GetHugePageSize();
    if (pageSize == 0) {
        GTEST_SKIP() << "System huge page size is unavailable";
    }

    auto blobSize = manager->GetHugePageBlobSize();
    auto cachedBlob = manager->ReserveHugePageBlob().ValueOrThrow();
    auto outstandingBlob = manager->ReserveHugePageBlob().ValueOrThrow();
    outstandingBlob[0] = 'x';
    auto alias = outstandingBlob.Slice(0, 1);
    cachedBlob.Reset();
    EXPECT_EQ(manager->GetUsedHugePageCount(), 2 * config->PagesPerBlob);
    EXPECT_EQ(tracker->GetUsed(), 2 * blobSize);

    manager.Reset();
    EXPECT_EQ(tracker->GetUsed(), blobSize);
    outstandingBlob.Reset();
    EXPECT_EQ(alias[0], 'x');
    EXPECT_EQ(tracker->GetUsed(), blobSize);
    alias.Reset();
    EXPECT_EQ(tracker->GetUsed(), 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NIO
