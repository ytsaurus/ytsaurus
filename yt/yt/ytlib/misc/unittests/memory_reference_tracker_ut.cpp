#include <yt/yt/ytlib/misc/memory_reference_tracker.h>
#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/core/misc/memory_reference_tracker.h>

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

class TTypedMemoryTracker
    : public ITypedNodeMemoryTracker
{
public:
    TTypedMemoryTracker(
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

private:
    const INodeMemoryTrackerPtr MemoryTracker_;
    const EMemoryCategory Category_;
};

class TMockNodeMemoryTracker
    : public INodeMemoryTracker
{
public:
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

    void ClearTrackers()
    {
        YT_ABORT();
    }

    i64 GetExplicitLimit(EMemoryCategory /*category*/) const
    {
        YT_ABORT();
    }

    i64 GetLimit(EMemoryCategory /*category*/, const std::optional<TPoolTag>& /*poolTag*/ = {}) const
    {
        YT_ABORT();
    }

    i64 GetUsed(EMemoryCategory /*category*/, const std::optional<TPoolTag>& /*poolTag*/ = {}) const
    {
        YT_ABORT();
    }

    i64 GetFree(EMemoryCategory /*category*/, const std::optional<TPoolTag>& /*poolTag*/ = {}) const
    {
        YT_ABORT();
    }

    bool IsExceeded(EMemoryCategory /*category*/, const std::optional<TPoolTag>& /*poolTag*/ = {}) const
    {
        YT_ABORT();
    }

    void SetTotalLimit(i64 /*newLimit*/)
    {
        YT_ABORT();
    }

    void SetCategoryLimit(EMemoryCategory /*category*/, i64 /*newLimit*/)
    {
        YT_ABORT();
    }

    void SetPoolWeight(const TPoolTag& /*poolTag*/, i64 /*newWeight*/)
    {
        YT_ABORT();
    }


    bool Acquire(EMemoryCategory category, i64 size, const std::optional<TPoolTag>& /*poolTag*/)
    {
        YT_LOG_DEBUG("Acquire(%v, %v)", category, size);
        CategoryToUsage_[category] += size;
        return false;
    }

    TError TryAcquire(EMemoryCategory /*category*/, i64 /*size*/, const std::optional<TPoolTag>& /*poolTag*/)
    {
        YT_ABORT();
    }

    TError TryChange(EMemoryCategory /*category*/, i64 /*size*/, const std::optional<TPoolTag>& /*poolTag*/)
    {
        YT_ABORT();
    }

    void Release(EMemoryCategory category, i64 size, const std::optional<TPoolTag>& /*poolTag*/)
    {
        YT_LOG_DEBUG("Release(%v, %v)", category, size);
        CategoryToUsage_[category] -= size;
    }

    i64 UpdateUsage(EMemoryCategory /*category*/, i64 /*newUsage*/)
    {
        YT_ABORT();
    }

    ITypedNodeMemoryTrackerPtr WithCategory(
        EMemoryCategory category,
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

        for (auto [someCategory, someSize] : CategoryToUsage_) {
            if (someCategory != category && someSize != 0) {
                return false;
            }
        }

        return true;
    }

    bool IsEmpty()
    {
        for (auto [someCategory, someSize] : CategoryToUsage_) {
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

TEST(TMemoryReferenceTrackerHelpersTest, ReferenceCreation)
{
    CreateReference(239);
}

TEST(TMemoryReferenceTrackerHelpersTest, Tracker)
{
    auto tracker = New<TMockNodeMemoryTracker>();
    auto category = EMemoryCategory::BlockCache;

    EXPECT_TRUE(tracker->IsEmpty());
    tracker->Acquire(category, 1, std::nullopt);
    EXPECT_TRUE(tracker->CheckMemoryUsage(category, 1));
    EXPECT_FALSE(tracker->IsEmpty());
}

TEST(TMemoryReferenceTrackerTest, Register)
{
    auto memoryTracker = New<TMockNodeMemoryTracker>();
    auto referenceTracker = CreateNodeMemoryReferenceTracker(memoryTracker);

    {
        auto reference = CreateReference(1);
        EXPECT_TRUE(memoryTracker->IsEmpty());
        reference = referenceTracker->WithCategory()->Track(std::move(reference));
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::Unknown, 1));
        auto mirrorReference = referenceTracker->WithCategory()->Track(reference);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::Unknown, 1));
    }

    EXPECT_TRUE(memoryTracker->IsEmpty());
}

TEST(TMemoryReferenceTrackerTest, Acquire)
{
    auto memoryTracker = New<TMockNodeMemoryTracker>();
    auto referenceTracker = CreateNodeMemoryReferenceTracker(memoryTracker);

    {
        auto reference = CreateReference(1);
        EXPECT_TRUE(memoryTracker->IsEmpty());
        reference = referenceTracker->WithCategory()->Track(std::move(reference));
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::Unknown, 1));

        auto referenceP2p = referenceTracker->WithCategory(EMemoryCategory::P2P)->Track(reference);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::P2P, 1));

        auto referenceCache = referenceTracker->WithCategory(EMemoryCategory::MasterCache)->Track(reference);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::Mixed, 1));
    }

    EXPECT_TRUE(memoryTracker->IsEmpty());
}

TEST(TMemoryReferenceTrackerTest, AcquireRelease)
{
    auto memoryTracker = New<TMockNodeMemoryTracker>();
    auto referenceTracker = CreateNodeMemoryReferenceTracker(memoryTracker);

    {
        auto reference = CreateReference(1);
        EXPECT_TRUE(memoryTracker->IsEmpty());
        reference = referenceTracker->WithCategory()->Track(std::move(reference));
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::Unknown, 1));

        auto referenceP2p = referenceTracker->WithCategory(EMemoryCategory::P2P)->Track(reference);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::P2P, 1));

        auto referenceCache = referenceTracker->WithCategory(EMemoryCategory::MasterCache)->Track(reference);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::Mixed, 1));

        referenceP2p.Reset();
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::MasterCache, 1));

        referenceCache.Reset();
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::Unknown, 1));
    }

    EXPECT_TRUE(memoryTracker->IsEmpty());
}

TEST(TMemoryReferenceTrackerTest, BlockCache)
{
    auto memoryTracker = New<TMockNodeMemoryTracker>();
    auto referenceTracker = CreateNodeMemoryReferenceTracker(memoryTracker);

    {
        auto reference = CreateReference(1);
        EXPECT_TRUE(memoryTracker->IsEmpty());
        reference = referenceTracker->WithCategory()->Track(std::move(reference));
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::Unknown, 1));

        auto referenceBlockCache = referenceTracker->WithCategory(EMemoryCategory::BlockCache)->Track(reference);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::BlockCache, 1));

        auto referenceP2p = referenceTracker->WithCategory(EMemoryCategory::P2P)->Track(reference);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::P2P, 1));

        auto referenceMasterCache = referenceTracker->WithCategory(EMemoryCategory::MasterCache)->Track(reference);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::Mixed, 1));

        referenceP2p.Reset();
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::MasterCache, 1));

        referenceMasterCache.Reset();
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::BlockCache, 1));
    }

    EXPECT_TRUE(memoryTracker->IsEmpty());
}

TEST(TMemoryReferenceTrackerTest, ResetCategory)
{
    auto memoryTracker = New<TMockNodeMemoryTracker>();
    auto referenceTracker = CreateNodeMemoryReferenceTracker(memoryTracker);

    {
        auto reference = CreateReference(1);
        EXPECT_TRUE(memoryTracker->IsEmpty());
        reference = TrackMemory(referenceTracker, EMemoryCategory::P2P, std::move(reference));
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::P2P, 1));
        reference = TrackMemory(referenceTracker, EMemoryCategory::MasterCache, std::move(reference));
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::MasterCache, 1));
    }

    EXPECT_TRUE(memoryTracker->IsEmpty());
}

TEST(TMemoryReferenceTrackerTest, AttachCategory)
{
    auto memoryTracker = New<TMockNodeMemoryTracker>();
    auto referenceTracker = CreateNodeMemoryReferenceTracker(memoryTracker);

    {
        auto reference = CreateReference(1);
        EXPECT_TRUE(memoryTracker->IsEmpty());
        reference = TrackMemory(referenceTracker, EMemoryCategory::P2P, std::move(reference));
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::P2P, 1));
        reference = TrackMemory(referenceTracker, EMemoryCategory::MasterCache, std::move(reference), true);
        EXPECT_TRUE(memoryTracker->CheckMemoryUsage(EMemoryCategory::Mixed, 1));
    }

    EXPECT_TRUE(memoryTracker->IsEmpty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
