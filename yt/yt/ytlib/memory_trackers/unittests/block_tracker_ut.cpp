#include <yt/yt/ytlib/chunk_client/block.h>

#include <yt/yt/ytlib/memory_trackers/block_tracker.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>

namespace NYT {

namespace {

using NChunkClient::TBlock;

////////////////////////////////////////////////////////////////////////////////

TSharedRef CreateBlock(i64 size)
{
    TString s;
    s.resize(size, '*');

    auto output = TSharedRef::FromString(s);
    YT_ASSERT(static_cast<i64>(output.Size()) == size);
    return output;
}

////////////////////////////////////////////////////////////////////////////////

class TTypedMemoryTracker
    : public IMemoryUsageTracker
{
public:
    using ECategory = INodeMemoryTracker::ECategory;

    TTypedMemoryTracker(
        INodeMemoryTrackerPtr memoryTracker,
        ECategory category)
        : MemoryTracker_(std::move(memoryTracker))
        , Category_(category)
    { }

    TError TryAcquire(i64 /*size*/) override
    {
        YT_ABORT();
    }

    TError TryChange(i64 /*size*/) override
    {
        YT_ABORT();
    }

    void Acquire(i64 size) override
    {
        MemoryTracker_->Acquire(Category_, size);
    }

    void Release(i64 size) override
    {
        MemoryTracker_->Release(Category_, size);
    }

    void SetLimit(i64 /*size*/) override
    {
        YT_ABORT();
    }

private:
    const INodeMemoryTrackerPtr MemoryTracker_;
    const ECategory Category_;
};

class TMockNodeMemoryTracker
    : public INodeMemoryTracker
{
public:
    using ECategory = INodeMemoryTracker::ECategory;

    TMockNodeMemoryTracker()
        : Logger("TMockSimpleMemoryUsageTracker")
    {
        YT_LOG_DEBUG("Created TMockSimpleMemoryUsageTracker");
    }

    i64 GetTotalLimit() const
    {
        YT_ABORT();
    }

    i64 GetTotalUsed() const
    {
        YT_ABORT();
    }

    i64 GetTotalFree() const
    {
        YT_ABORT();
    }

    bool IsTotalExceeded() const
    {
        YT_ABORT();
    }

    i64 GetExplicitLimit(ECategory /*category*/) const
    {
        YT_ABORT();
    }

    i64 GetLimit(ECategory /*category*/, const std::optional<TPoolTag>& /*poolTag*/ = {}) const
    {
        YT_ABORT();
    }

    i64 GetUsed(ECategory /*category*/, const std::optional<TPoolTag>& /*poolTag*/ = {}) const
    {
        YT_ABORT();
    }

    i64 GetFree(ECategory /*category*/, const std::optional<TPoolTag>& /*poolTag*/ = {}) const
    {
        YT_ABORT();
    }

    bool IsExceeded(ECategory /*category*/, const std::optional<TPoolTag>& /*poolTag*/ = {}) const
    {
        YT_ABORT();
    }

    void SetTotalLimit(i64 /*newLimit*/)
    {
        YT_ABORT();
    }

    void SetCategoryLimit(ECategory /*category*/, i64 /*newLimit*/)
    {
        YT_ABORT();
    }

    void SetPoolWeight(const TPoolTag& /*poolTag*/, i64 /*newWeight*/)
    {
        YT_ABORT();
    }


    void Acquire(ECategory category, i64 size, const std::optional<TPoolTag>& /*poolTag*/)
    {
        YT_LOG_DEBUG("Acquire(%v, %v)", category, size);
        CategoryToUsage_[category] += size;
    }

    TError TryAcquire(ECategory /*category*/, i64 /*size*/, const std::optional<TPoolTag>& /*poolTag*/)
    {
        YT_ABORT();
    }

    TError TryChange(ECategory /*category*/, i64 /*size*/, const std::optional<TPoolTag>& /*poolTag*/)
    {
        YT_ABORT();
    }

    void Release(ECategory category, i64 size, const std::optional<TPoolTag>& /*poolTag*/)
    {
        YT_LOG_DEBUG("Release(%v, %v)", category, size);
        CategoryToUsage_[category] -= size;
    }

    i64 UpdateUsage(ECategory /*category*/, i64 /*newUsage*/)
    {
        YT_ABORT();
    }

    IMemoryUsageTrackerPtr WithCategory(
        ECategory category,
        std::optional<TPoolTag> poolTag)
    {
        YT_VERIFY(!poolTag);
        return New<TTypedMemoryTracker>(this, category);
    }

    bool CheckMemoryUsage(EMemoryCategory category, i64 size)
    {
        if (CategoryToUsage_[category] != size) {
            return false;
        }

        for (auto& [someCategory, someSize]: CategoryToUsage_) {
            if (someCategory != category && someSize != 0) {
                return false;
            }
        }

        return true;
    }

    bool IsEmpty()
    {
        for (auto& [someCategory, someSize]: CategoryToUsage_) {
            if (someSize != 0) {
                return false;
            }
        }

        return true;
    }
private:
    const NLogging::TLogger Logger;
    THashMap<EMemoryCategory, i64> CategoryToUsage_;
};

////////////////////////////////////////////////////////////////////////////////

TEST(TBlockTrackerHelpersTest, BlockCreation)
{
    CreateBlock(239);
}

TEST(TBlockTrackerHelpersTest, Tracker)
{
    auto tracker = New<TMockNodeMemoryTracker>();
    auto category = EMemoryCategory::BlockCache;

    EXPECT_TRUE(tracker->IsEmpty());
    tracker->Acquire(category, 1, std::nullopt);
    EXPECT_TRUE(tracker->CheckMemoryUsage(category, 1));
    EXPECT_FALSE(tracker->IsEmpty());
}

TEST(TBlockTrackerTest, Register)
{
    auto memoryTracker = New<TMockNodeMemoryTracker>();
    auto blockTracker = CreateBlockTracker(memoryTracker);

    {
        auto block_1 = CreateBlock(1);
        EXPECT_TRUE(memoryTracker->IsEmpty());
        block_1 = blockTracker->RegisterBlock(std::move(block_1));
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::UnknownBlocks, 1));
        auto block_2 = blockTracker->RegisterBlock(block_1);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::UnknownBlocks, 1));
    }
    EXPECT_TRUE(memoryTracker->IsEmpty());
}

TEST(TBlockTrackerTest, Acquire)
{
    auto memoryTracker = New<TMockNodeMemoryTracker>();
    auto blockTracker = CreateBlockTracker(memoryTracker);

    {
        auto block = CreateBlock(1);
        EXPECT_TRUE(memoryTracker->IsEmpty());
        block = blockTracker->RegisterBlock(std::move(block));
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::UnknownBlocks, 1));

        blockTracker->AcquireCategory(block, EMemoryCategory::P2P);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::P2P, 1));

        blockTracker->AcquireCategory(block, EMemoryCategory::MasterCache);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::MixedBlocks, 1));
    }
    EXPECT_TRUE(memoryTracker->IsEmpty());
}

TEST(TBlockTrackerTest, AcquireRelease)
{
    auto memoryTracker = New<TMockNodeMemoryTracker>();
    auto blockTracker = CreateBlockTracker(memoryTracker);

    {
        auto block = CreateBlock(1);
        EXPECT_TRUE(memoryTracker->IsEmpty());
        block = blockTracker->RegisterBlock(std::move(block));
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::UnknownBlocks, 1));

        blockTracker->AcquireCategory(block, EMemoryCategory::P2P);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::P2P, 1));

        blockTracker->AcquireCategory(block, EMemoryCategory::MasterCache);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::MixedBlocks, 1));

        blockTracker->ReleaseCategory(block, EMemoryCategory::P2P);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::MasterCache, 1));

        blockTracker->ReleaseCategory(block, EMemoryCategory::MasterCache);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::UnknownBlocks, 1));
    }
    EXPECT_TRUE(memoryTracker->IsEmpty());
}

TEST(TBlockTrackerTest, BlockCache)
{
    auto memoryTracker = New<TMockNodeMemoryTracker>();
    auto blockTracker = CreateBlockTracker(memoryTracker);

    {
        auto block = CreateBlock(1);
        EXPECT_TRUE(memoryTracker->IsEmpty());
        block = blockTracker->RegisterBlock(std::move(block));
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::UnknownBlocks, 1));

        blockTracker->AcquireCategory(block, EMemoryCategory::BlockCache);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::BlockCache, 1));

        blockTracker->AcquireCategory(block, EMemoryCategory::P2P);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::P2P, 1));

        blockTracker->AcquireCategory(block, EMemoryCategory::MasterCache);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::MixedBlocks, 1));

        blockTracker->ReleaseCategory(block, EMemoryCategory::P2P);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::MasterCache, 1));

        blockTracker->ReleaseCategory(block, EMemoryCategory::MasterCache);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::BlockCache, 1));
    }
    EXPECT_TRUE(memoryTracker->IsEmpty());
}

TEST(TBlockTrackerTest, ResetCategory)
{
    auto memoryTracker = New<TMockNodeMemoryTracker>();
    auto blockTracker = CreateBlockTracker(memoryTracker);

    {
        auto block = CreateBlock(1);
        EXPECT_TRUE(memoryTracker->IsEmpty());
        block = ResetCategory(std::move(block), blockTracker, EMemoryCategory::P2P);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::P2P, 1));
        block = ResetCategory(std::move(block), blockTracker, EMemoryCategory::MasterCache);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::MasterCache, 1));
    }
    EXPECT_TRUE(memoryTracker->IsEmpty());
}

TEST(TBlockTrackerTest, AttachCategory)
{
    auto memoryTracker = New<TMockNodeMemoryTracker>();
    auto blockTracker = CreateBlockTracker(memoryTracker);

    {
        auto block = CreateBlock(1);
        EXPECT_TRUE(memoryTracker->IsEmpty());
        block = AttachCategory(std::move(block), blockTracker, EMemoryCategory::P2P);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::P2P, 1));
        block = AttachCategory(std::move(block), blockTracker, EMemoryCategory::MasterCache);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::MixedBlocks, 1));
    }
    EXPECT_TRUE(memoryTracker->IsEmpty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
