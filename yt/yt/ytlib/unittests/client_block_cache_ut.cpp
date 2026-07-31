#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NChunkClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TClientBlockCacheTest
    : public ::testing::Test
{
protected:
    static constexpr i64 CacheCapacity = 1_KB;
    static constexpr i64 OversizedBlockSize = 2_KB;

    INodeMemoryTrackerPtr NodeMemoryTracker_ = CreateNodeMemoryTracker(
        32_MB,
        New<TNodeMemoryTrackerConfig>());
    IMemoryUsageTrackerPtr MemoryUsageTracker_ =
        NodeMemoryTracker_->WithCategory(EMemoryCategory::BlockCache);
    IClientBlockCachePtr BlockCache_ = CreateClientBlockCache(
        CreateBlockCacheConfig(),
        EBlockType::CompressedData,
        MemoryUsageTracker_);

    void TearDown() override
    {
        BlockCache_.Reset();
        MemoryUsageTracker_.Reset();
        NodeMemoryTracker_->ClearTrackers();
        NodeMemoryTracker_.Reset();
    }

    static TBlockCacheConfigPtr CreateBlockCacheConfig()
    {
        auto config = New<TBlockCacheConfig>();
        config->CompressedData->Capacity = CacheCapacity;
        return config;
    }

    static TBlockId CreateBlockId()
    {
        return TBlockId(
            NObjectClient::MakeRandomId(
                NObjectClient::EObjectType::Chunk,
                NObjectClient::TCellTag(0xf003)),
            0);
    }

    static TCachedBlock CreateOversizedBlock()
    {
        return TCachedBlock(TSharedRef::FromString(std::string(OversizedBlockSize, 'x')));
    }
};

TEST_F(TClientBlockCacheTest, ProducerCookieDoesNotRetainRejectedBlock)
{
    auto blockId = CreateBlockId();
    auto producerCookie = BlockCache_->GetBlockCookie(blockId, EBlockType::CompressedData);
    ASSERT_TRUE(producerCookie->IsActive());

    producerCookie->SetBlock(CreateOversizedBlock());

    EXPECT_EQ(0, MemoryUsageTracker_->GetUsed());
    EXPECT_FALSE(BlockCache_->FindBlock(blockId, EBlockType::CompressedData));
}

TEST_F(TClientBlockCacheTest, WaiterCookieRetainsRejectedBlockUntilReleased)
{
    auto blockId = CreateBlockId();
    auto producerCookie = BlockCache_->GetBlockCookie(blockId, EBlockType::CompressedData);
    ASSERT_TRUE(producerCookie->IsActive());

    auto waiterCookie = BlockCache_->GetBlockCookie(blockId, EBlockType::CompressedData);
    ASSERT_FALSE(waiterCookie->IsActive());

    producerCookie->SetBlock(CreateOversizedBlock());

    NConcurrency::WaitFor(waiterCookie->GetBlockFuture()).ThrowOnError();
    EXPECT_EQ(OversizedBlockSize, MemoryUsageTracker_->GetUsed());

    auto block = waiterCookie->GetBlock();
    EXPECT_EQ(OversizedBlockSize, block.Size());

    waiterCookie.reset();
    EXPECT_EQ(OversizedBlockSize, MemoryUsageTracker_->GetUsed());

    block = {};
    EXPECT_EQ(0, MemoryUsageTracker_->GetUsed());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkClient
