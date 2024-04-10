#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TSharedRef CreateReference(i64 size)
{
    TString s;
    s.resize(size, '*');

    auto output = TSharedRef::FromString(s);
    YT_ASSERT(static_cast<i64>(output.Size()) == size);
    return output;
}

////////////////////////////////////////////////////////////////////////////////

class TMemoryUsageTracker
    : public IMemoryUsageTracker
{
public:
    TMemoryUsageTracker(
        INodeMemoryTrackerPtr memoryTracker,
        EMemoryCategory category)
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

    bool Acquire(i64 size) override
    {
        return MemoryTracker_->Acquire(Category_, size);
    }

    void Release(i64 size) override
    {
        MemoryTracker_->Release(Category_, size);
    }

    void SetLimit(i64 /*size*/) override
    {
        YT_ABORT();
    }

    i64 GetLimit() const override
    {
        YT_ABORT();
    }

    i64 GetUsed() const override
    {
        YT_ABORT();
    }

    i64 GetFree() const override
    {
        YT_ABORT();
    }

    bool IsExceeded() const override
    {
        YT_ABORT();
    }

    TSharedRef Track(TSharedRef reference, bool keepExistingTracker) override
    {
        return MemoryTracker_->Track(reference, Category_, keepExistingTracker);
    }

private:
    const INodeMemoryTrackerPtr MemoryTracker_;
    const EMemoryCategory Category_;
};

class TTestNodeMemoryTracker
    : public INodeMemoryTracker
{
public:
    TTestNodeMemoryTracker()
        : Underlying_(CreateNodeMemoryTracker(
            std::numeric_limits<i64>::max(),
            {}))
    { }

    i64 GetTotalLimit() const
    {
        return Underlying_->GetTotalLimit();
    }

    i64 GetTotalUsed() const
    {
        return Underlying_->GetTotalUsed();
    }

    i64 GetTotalFree() const
    {
        return Underlying_->GetTotalFree();
    }

    bool IsTotalExceeded() const
    {
        return Underlying_->IsTotalExceeded();
    }

    void ClearTrackers()
    {
        Underlying_->ClearTrackers();
    }

    i64 GetExplicitLimit(EMemoryCategory category) const
    {
        return Underlying_->GetExplicitLimit(category);
    }

    i64 GetLimit(EMemoryCategory category, const std::optional<TPoolTag>& poolTag = {}) const
    {
        return Underlying_->GetLimit(category, poolTag);
    }

    i64 GetUsed(EMemoryCategory category, const std::optional<TPoolTag>& poolTag = {}) const
    {
        return Underlying_->GetUsed(category, poolTag);
    }

    i64 GetFree(EMemoryCategory category, const std::optional<TPoolTag>& poolTag = {}) const
    {
        return Underlying_->GetFree(category, poolTag);
    }

    bool IsExceeded(EMemoryCategory category, const std::optional<TPoolTag>& poolTag = {}) const
    {
        return Underlying_->IsExceeded(category, poolTag);
    }

    void SetTotalLimit(i64 newLimit)
    {
        Underlying_->SetTotalLimit(newLimit);
    }

    void SetCategoryLimit(EMemoryCategory category, i64 newLimit)
    {
        Underlying_->SetCategoryLimit(category, newLimit);
    }

    void SetPoolWeight(const TPoolTag& poolTag, i64 newWeight)
    {
        Underlying_->SetPoolWeight(poolTag, newWeight);
    }

    bool Acquire(EMemoryCategory category, i64 size, const std::optional<TPoolTag>& poolTag)
    {
        return Underlying_->Acquire(category, size, poolTag);
    }

    TError TryAcquire(EMemoryCategory category, i64 size, const std::optional<TPoolTag>& poolTag)
    {
        return Underlying_->TryAcquire(category, size, poolTag);
    }

    TError TryChange(EMemoryCategory category, i64 size, const std::optional<TPoolTag>& poolTag)
    {
        return Underlying_->TryChange(category, size, poolTag);
    }

    void Release(EMemoryCategory category, i64 size, const std::optional<TPoolTag>& poolTag)
    {
        return Underlying_->Release(category, size, poolTag);
    }

    i64 UpdateUsage(EMemoryCategory category, i64 newUsage)
    {
        return Underlying_->UpdateUsage(category, newUsage);
    }

    IMemoryUsageTrackerPtr WithCategory(
        EMemoryCategory category,
        std::optional<TPoolTag> poolTag = {})
    {
        return Underlying_->WithCategory(category, poolTag);
    }

    bool CheckMemoryUsage(EMemoryCategory category, i64 size)
    {
        if (Underlying_->GetUsed(category) != size) {
            return false;
        }

        for (auto someCategory : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
            if (someCategory != category && Underlying_->GetUsed(someCategory) != 0) {
                return false;
            }
        }

        return true;
    }

    bool IsEmpty()
    {
        for (auto someCategory : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
            if (Underlying_->GetUsed(someCategory) != 0) {
                return false;
            }
        }

        return true;
    }

    TSharedRef Track(
        TSharedRef reference,
        EMemoryCategory category,
        bool keepHolder)
    {
        return Underlying_->Track(reference, category, keepHolder);
    }

private:
    const INodeMemoryTrackerPtr Underlying_;
};

////////////////////////////////////////////////////////////////////////////////

TEST(TMemoryUsageTrackerHelpersTest, ReferenceCreation)
{
    CreateReference(239);
}

TEST(TMemoryUsageTrackerHelpersTest, Tracker)
{
    auto tracker = New<TTestNodeMemoryTracker>();
    auto category = EMemoryCategory::BlockCache;

    EXPECT_TRUE(tracker->IsEmpty());
    tracker->Acquire(category, 1, std::nullopt);
    EXPECT_TRUE(tracker->CheckMemoryUsage(category, 1));
    EXPECT_FALSE(tracker->IsEmpty());
    tracker->ClearTrackers();
}

TEST(TMemoryUsageTrackerTest, Register)
{
    auto memoryTracker = New<TTestNodeMemoryTracker>();

    {
        auto reference = CreateReference(1);
        EXPECT_TRUE(memoryTracker->IsEmpty());
        reference = memoryTracker->WithCategory(EMemoryCategory::Unknown)->Track(std::move(reference));
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::Unknown, 1));
        auto mirrorReference = memoryTracker->WithCategory(EMemoryCategory::Unknown)->Track(reference);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::Unknown, 1));
    }

    EXPECT_TRUE(memoryTracker->IsEmpty());
    memoryTracker->ClearTrackers();
}

TEST(TMemoryUsageTrackerTest, Acquire)
{
    auto memoryTracker = New<TTestNodeMemoryTracker>();

    {
        auto reference = CreateReference(1);
        EXPECT_TRUE(memoryTracker->IsEmpty());
        reference = memoryTracker->WithCategory(EMemoryCategory::Unknown)->Track(std::move(reference));
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::Unknown, 1));

        auto referenceP2p = memoryTracker->WithCategory(EMemoryCategory::P2P)->Track(reference);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::P2P, 1));

        auto referenceCache = memoryTracker->WithCategory(EMemoryCategory::MasterCache)->Track(reference);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::Mixed, 1));
    }

    EXPECT_TRUE(memoryTracker->IsEmpty());
    memoryTracker->ClearTrackers();
}

TEST(TMemoryUsageTrackerTest, AcquireRelease)
{
    auto memoryTracker = New<TTestNodeMemoryTracker>();

    {
        auto reference = CreateReference(1);
        EXPECT_TRUE(memoryTracker->IsEmpty());
        reference = memoryTracker->WithCategory(EMemoryCategory::Unknown)->Track(std::move(reference));
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::Unknown, 1));

        auto referenceP2p = memoryTracker->WithCategory(EMemoryCategory::P2P)->Track(reference);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::P2P, 1));

        auto referenceCache = memoryTracker->WithCategory(EMemoryCategory::MasterCache)->Track(reference);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::Mixed, 1));

        referenceP2p.Reset();
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::MasterCache, 1));

        referenceCache.Reset();
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::Unknown, 1));
    }

    EXPECT_TRUE(memoryTracker->IsEmpty());
    memoryTracker->ClearTrackers();
}

TEST(TMemoryUsageTrackerTest, BlockCache)
{
    auto memoryTracker = New<TTestNodeMemoryTracker>();

    {
        auto reference = CreateReference(1);
        EXPECT_TRUE(memoryTracker->IsEmpty());
        reference = memoryTracker->WithCategory(EMemoryCategory::Unknown)->Track(std::move(reference));
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::Unknown, 1));

        auto referenceBlockCache = memoryTracker->WithCategory(EMemoryCategory::BlockCache)->Track(reference);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::BlockCache, 1));

        auto referenceP2p = memoryTracker->WithCategory(EMemoryCategory::P2P)->Track(reference);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::P2P, 1));

        auto referenceMasterCache = memoryTracker->WithCategory(EMemoryCategory::MasterCache)->Track(reference);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::Mixed, 1));

        referenceP2p.Reset();
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::MasterCache, 1));

        referenceMasterCache.Reset();
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::BlockCache, 1));
    }

    EXPECT_TRUE(memoryTracker->IsEmpty());
    memoryTracker->ClearTrackers();
}

TEST(TMemoryUsageTrackerTest, ResetCategory)
{
    auto memoryTracker = New<TTestNodeMemoryTracker>();

    {
        auto reference = CreateReference(1);
        EXPECT_TRUE(memoryTracker->IsEmpty());
        reference = TrackMemory(memoryTracker, EMemoryCategory::P2P, std::move(reference));
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::P2P, 1));
        reference = TrackMemory(memoryTracker, EMemoryCategory::MasterCache, std::move(reference));
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::MasterCache, 1));
    }

    EXPECT_TRUE(memoryTracker->IsEmpty());
    memoryTracker->ClearTrackers();
}

TEST(TMemoryUsageTrackerTest, AttachCategory)
{
    auto memoryTracker = New<TTestNodeMemoryTracker>();

    {
        auto reference = CreateReference(1);
        EXPECT_TRUE(memoryTracker->IsEmpty());
        reference = TrackMemory(memoryTracker, EMemoryCategory::P2P, std::move(reference));
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::P2P, 1));
        reference = TrackMemory(memoryTracker, EMemoryCategory::MasterCache, std::move(reference), true);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::Mixed, 1));
    }

    EXPECT_TRUE(memoryTracker->IsEmpty());
    memoryTracker->ClearTrackers();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
