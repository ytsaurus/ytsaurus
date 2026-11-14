#include <yt/yt/ytlib/object_client/config.h>
#include <yt/yt/ytlib/object_client/object_service_cache.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <thread>

namespace NYT::NObjectClient {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger Logger("Test");

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]]
NHydra::TRevision Increment(NHydra::TRevision revision)
{
    return NHydra::TRevision(revision.Underlying() + 1);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TObjectServiceCacheTest, StaleResponse)
{
    auto cache = New<TObjectServiceCache>(
        New<TObjectServiceCacheConfig>(),
        GetNullMemoryUsageTracker(),
        Logger,
        NProfiling::TProfiler());

    auto key = TObjectServiceCacheKey(
        TCellTag(0),
        "root", // user
        NYPath::TYPath("//sys"), // path
        "ObjectService", // service
        "Execute", // method
        TSharedRef::FromString(std::string("request")),
        /*suppressUpstreamSync*/ false,
        /*suppressTransactionCoordinatorSync*/ false);

    auto requestId = NRpc::TRequestId::Create();
    auto expirationTime = TDuration::MilliSeconds(10);
    auto data = TSharedRefArray(TSharedRef::FromString(std::string("response")));
    NHydra::TRevision currentRevision(1);

    auto beginLookup = [&] (TDuration stalenessBound, NHydra::TRevision revision) {
        return cache->BeginLookup(
            requestId,
            key,
            expirationTime,
            expirationTime,
            stalenessBound,
            revision);
    };

    auto endLookup = [&] (TObjectServiceCache::TCookie&& cookie) {
        currentRevision = Increment(currentRevision);
        cache->EndLookup(
            requestId,
            std::move(cookie),
            data,
            currentRevision,
            /*success*/ true);
    };

    {
        auto cookie1 = beginLookup(TDuration::Zero(), NHydra::NullRevision);

        // Cache is empty, nothing is found.
        EXPECT_TRUE(cookie1.IsActive());
        EXPECT_EQ(nullptr, cookie1.ExpiredEntry());

        endLookup(std::move(cookie1));

        // Value has not expired yet, return it usual way.
        auto cookie2 = beginLookup(TDuration::Seconds(1), NHydra::NullRevision);

        EXPECT_FALSE(cookie2.IsActive());
        EXPECT_EQ(nullptr, cookie2.ExpiredEntry());
        EXPECT_TRUE(cookie2.GetValue().IsSet());
        EXPECT_TRUE(TSharedRefArray::AreBitwiseEqual(data, WaitForFast(cookie2.GetValue()).Value()->GetResponseMessage()));
    }

    NConcurrency::TDelayedExecutor::WaitForDuration(5 * expirationTime);

    {
        // Stale response ruled out by both conditions.
        auto cookie1 = beginLookup(3 * expirationTime, Increment(currentRevision));

        EXPECT_TRUE(cookie1.IsActive());
        EXPECT_EQ(nullptr, cookie1.ExpiredEntry());

        // Stale response ruled out by staleness bound.
        auto cookie2 = beginLookup(3 * expirationTime, NHydra::NullRevision);
        EXPECT_FALSE(cookie2.IsActive());
        EXPECT_FALSE(cookie2.GetValue().IsSet());
        EXPECT_EQ(nullptr, cookie2.ExpiredEntry());

        // Stale response ruled out by revision.
        auto cookie3 = beginLookup(10 * expirationTime, Increment(currentRevision));

        EXPECT_FALSE(cookie3.IsActive());
        EXPECT_FALSE(cookie2.GetValue().IsSet());
        EXPECT_EQ(nullptr, cookie3.ExpiredEntry());

        // Stale response is ok for this request.
        auto cookie4 = beginLookup(10 * expirationTime, NHydra::NullRevision);
        EXPECT_FALSE(cookie4.IsActive());
        EXPECT_FALSE(cookie4.GetValue().IsSet());
        EXPECT_NE(nullptr, cookie4.ExpiredEntry());
        EXPECT_TRUE(TSharedRefArray::AreBitwiseEqual(data, cookie4.ExpiredEntry()->GetResponseMessage()));

        endLookup(std::move(cookie1));
    }
}

TEST(TObjectServiceCacheTest, StaleError)
{
    auto cache = New<TObjectServiceCache>(
        New<TObjectServiceCacheConfig>(),
        GetNullMemoryUsageTracker(),
        Logger,
        NProfiling::TProfiler());

    auto key = TObjectServiceCacheKey(
        TCellTag(0),
        "root", // user
        NYPath::TYPath("//sys"), // path
        "ObjectService", // service
        "Execute", // method
        TSharedRef::FromString(std::string("request")),
        /*suppressUpstreamSync*/ false,
        /*suppressTransactionCoordinatorSync*/ false);

    auto requestId = NRpc::TRequestId::Create();
    auto expirationTime = TDuration::MilliSeconds(10);
    auto data = TSharedRefArray(TSharedRef::FromString(std::string("response")));
    NHydra::TRevision currentRevision(1);

    auto beginLookup = [&] (TDuration stalenessBound, NHydra::TRevision revision) {
        return cache->BeginLookup(
            requestId,
            key,
            expirationTime,
            expirationTime,
            stalenessBound,
            revision);
    };

    auto endLookup = [&] (TObjectServiceCache::TCookie&& cookie) {
        currentRevision = Increment(currentRevision);
        cache->EndLookup(
            requestId,
            std::move(cookie),
            data,
            currentRevision,
            /*success*/ false);
    };

    {
        auto cookie1 = beginLookup(TDuration::Zero(), NHydra::NullRevision);

        // Cache is empty, nothing is found.
        EXPECT_TRUE(cookie1.IsActive());
        EXPECT_EQ(nullptr, cookie1.ExpiredEntry());

        endLookup(std::move(cookie1));

        // Value has not expired yet, return it usual way.
        auto cookie2 = beginLookup(TDuration::Seconds(1), NHydra::NullRevision);

        EXPECT_FALSE(cookie2.IsActive());
        EXPECT_EQ(nullptr, cookie2.ExpiredEntry());
        EXPECT_TRUE(cookie2.GetValue().IsSet());
        EXPECT_TRUE(TSharedRefArray::AreBitwiseEqual(data, WaitForFast(cookie2.GetValue()).Value()->GetResponseMessage()));
    }

    NConcurrency::TDelayedExecutor::WaitForDuration(5 * expirationTime);

    {
        // Stale response ruled out by both conditions.
        auto cookie1 = beginLookup(3 * expirationTime, Increment(currentRevision));

        EXPECT_TRUE(cookie1.IsActive());
        EXPECT_EQ(nullptr, cookie1.ExpiredEntry());

        // Stale response is ok for this request, but it is an error, so it is actually not.
        auto cookie2 = beginLookup(10 * expirationTime, NHydra::NullRevision);
        EXPECT_FALSE(cookie2.IsActive());
        EXPECT_FALSE(cookie2.GetValue().IsSet());
        EXPECT_EQ(nullptr, cookie2.ExpiredEntry());
    }
}

////////////////////////////////////////////////////////////////////////////////

// Keep the node tracker's real overcommit checks, including transient acquisitions
// that are released before EndLookup returns.
class TCheckedMemoryUsageTracker
    : public IMemoryUsageTracker
{
public:
    std::function<void(i64)> OnLimitChanged;

    explicit TCheckedMemoryUsageTracker(IMemoryUsageTrackerPtr underlying)
        : Underlying_(std::move(underlying))
    { }

    bool Acquire(i64 size) override
    {
        auto result = Underlying_->Acquire(size);
        EXPECT_TRUE(result);
        return result;
    }

    void Release(i64 size) override
    {
        Underlying_->Release(size);
        EXPECT_GE(Underlying_->GetUsed(), 0);
    }

    TError TryAcquire(i64 size) override
    {
        return Underlying_->TryAcquire(size);
    }

    TError TryChange(i64 size) override
    {
        return Underlying_->TryChange(size);
    }

    void SetLimit(i64 size) override
    {
        Underlying_->SetLimit(size);
        if (OnLimitChanged) {
            OnLimitChanged(size);
        }
    }

    void AdjustLimit(i64 size) override
    {
        Underlying_->AdjustLimit(size);
    }

    i64 GetLimit() const override
    {
        return Underlying_->GetLimit();
    }

    i64 GetUsed() const override
    {
        return Underlying_->GetUsed();
    }

    i64 GetFree() const override
    {
        return Underlying_->GetFree();
    }

    bool IsExceeded() const override
    {
        return Underlying_->IsExceeded();
    }

    TSharedRef Track(TSharedRef reference, bool keepExistingTracking) override
    {
        return Underlying_->Track(std::move(reference), keepExistingTracking);
    }

    TErrorOr<TSharedRef> TryTrack(TSharedRef reference, bool keepExistingTracking) override
    {
        return Underlying_->TryTrack(std::move(reference), keepExistingTracking);
    }

private:
    const IMemoryUsageTrackerPtr Underlying_;
};

////////////////////////////////////////////////////////////////////////////////

class TObjectServiceCacheMemoryTest
    : public ::testing::Test
{
protected:
    const TObjectServiceCacheConfigPtr Config_ = New<TObjectServiceCacheConfig>();
    TIntrusivePtr<TCheckedMemoryUsageTracker> MemoryTracker_;
    TObjectServiceCachePtr Cache_;

    const TObjectServiceCacheKey Key_ = TObjectServiceCacheKey(
        TCellTag(0),
        /*user*/ "root",
        /*path*/ "//sys",
        /*service*/ "ObjectService",
        /*method*/ "Execute",
        TSharedRef::FromString(std::string("request")),
        /*suppressUpstreamSync*/ false,
        /*suppressTransactionCoordinatorSync*/ false);
    const TSharedRefArray Response_ = TSharedRefArray(TSharedRef::FromString(std::string("response")));
    const NHydra::TRevision Revision_ = NHydra::TRevision(42);

    void SetUp() override
    {
        Config_->Capacity = 1_MB;
        auto trackerConfig = New<TNodeMemoryTrackerConfig>();
        trackerConfig->CheckPerCategoryLimitOvercommit = true;
        auto nodeTracker = CreateNodeMemoryTracker(/*totalLimit*/ 16_MB, trackerConfig);
        MemoryTracker_ = New<TCheckedMemoryUsageTracker>(nodeTracker->WithCategory(EMemoryCategory::MasterCache));
        Cache_ = New<TObjectServiceCache>(Config_, MemoryTracker_, Logger, NProfiling::TProfiler());
    }

    TObjectServiceCache::TCookie BeginLookup()
    {
        return Cache_->BeginLookup(
            NRpc::TRequestId::Create(),
            Key_,
            /*expireAfterSuccessfulUpdateTime*/ TDuration::Hours(1),
            /*expireAfterFailedUpdateTime*/ TDuration::Hours(1),
            /*successStalenessBound*/ TDuration::Zero(),
            NHydra::NullRevision);
    }

    void EndLookup(TObjectServiceCache::TCookie cookie, bool success = true)
    {
        Cache_->EndLookup(NRpc::TRequestId::Create(), std::move(cookie), Response_, Revision_, success);
    }

    void Reconfigure(std::optional<i64> capacity)
    {
        auto config = New<TObjectServiceCacheDynamicConfig>();
        config->Capacity = capacity;
        config->TopEntryByteRateThreshold = 0;
        Cache_->Reconfigure(config);
    }

    int GetTopEntryCount()
    {
        return NYTree::ConvertToNode(NYTree::SyncYPathGet(Cache_->GetOrchidService(), "/top_requests"))->AsList()->GetChildCount();
    }
};

TEST_F(TObjectServiceCacheMemoryTest, ZeroCapacityCompletesLookupsWithoutOvercommit)
{
    Reconfigure(Config_->Capacity);
    EndLookup(BeginLookup());
    auto retainedEntry = WaitForFast(BeginLookup().GetValue())
        .ValueOrThrow();
    EXPECT_GT(MemoryTracker_->GetUsed(), 0);
    EXPECT_EQ(1, GetTopEntryCount());

    Reconfigure(/*capacity*/ 0);
    EXPECT_EQ(0, MemoryTracker_->GetLimit());
    EXPECT_EQ(0, MemoryTracker_->GetUsed());
    EXPECT_EQ(0, GetTopEntryCount());

    for (int iteration = 0; iteration < 3; ++iteration) {
        auto cookie = BeginLookup();
        ASSERT_TRUE(cookie.IsActive());
        EXPECT_FALSE(cookie.ExpiredEntry());
        auto waiter = BeginLookup();
        EXPECT_FALSE(waiter.IsActive());
        EXPECT_FALSE(waiter.GetValue().IsSet());

        // Reconfiguration must preserve pending lookups and their concurrent waiters.
        Reconfigure(/*capacity*/ 0);
        bool success = iteration % 2 == 0;
        EndLookup(std::move(cookie), success);
        auto entry = WaitForFast(waiter.GetValue())
            .ValueOrThrow();
        EXPECT_EQ(Revision_, entry->GetRevision());
        EXPECT_EQ(success, entry->GetSuccess());
        EXPECT_TRUE(TSharedRefArray::AreBitwiseEqual(Response_, entry->GetResponseMessage()));
        EXPECT_EQ(0, MemoryTracker_->GetUsed());
        EXPECT_EQ(0, GetTopEntryCount());
    }
}

TEST_F(TObjectServiceCacheMemoryTest, ReenableRestoresMemoryLimit)
{
    for (auto capacity : {std::optional<i64>(256_KB), std::optional<i64>()}) {
        Reconfigure(/*capacity*/ 0);
        auto cookie = BeginLookup();
        ASSERT_TRUE(cookie.IsActive());
        auto waiter = BeginLookup();

        Reconfigure(capacity);
        EXPECT_EQ(capacity.value_or(1_MB), MemoryTracker_->GetLimit());
        EndLookup(std::move(cookie));
        auto entry = WaitForFast(waiter.GetValue())
            .ValueOrThrow();
        EXPECT_GT(MemoryTracker_->GetUsed(), 0);

        auto hit = BeginLookup();
        EXPECT_FALSE(hit.IsActive());
        EXPECT_TRUE(hit.GetValue().IsSet());
        EXPECT_EQ(
            entry,
            WaitForFast(hit.GetValue())
                .ValueOrThrow());
    }

    Reconfigure(/*capacity*/ 0);
    EXPECT_EQ(0, MemoryTracker_->GetUsed());
}

TEST_F(TObjectServiceCacheMemoryTest, LookupCompletingWhileZeroLimitIsInstalled)
{
    auto cookie = BeginLookup();
    auto waiter = BeginLookup();
    ASSERT_TRUE(cookie.IsActive());

    MemoryTracker_->OnLimitChanged = [&] (i64 limit) {
        if (limit == 0) {
            std::thread worker([&] {
                EndLookup(std::move(cookie));
            });
            worker.join();
        }
    };
    Reconfigure(/*capacity*/ 0);
    MemoryTracker_->OnLimitChanged = {};

    auto entry = WaitForFast(waiter.GetValue())
        .ValueOrThrow();
    EXPECT_TRUE(TSharedRefArray::AreBitwiseEqual(Response_, entry->GetResponseMessage()));
    EXPECT_EQ(0, MemoryTracker_->GetUsed());
    EXPECT_TRUE(BeginLookup().IsActive());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NObjectClient
