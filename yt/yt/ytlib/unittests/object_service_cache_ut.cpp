#include <yt/yt/ytlib/object_client/config.h>
#include <yt/yt/ytlib/object_client/object_service_cache.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NObjectClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger Logger("Test");

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]]
NHydra::TRevision Increment(NHydra::TRevision revision)
{
    return NHydra::TRevision(revision.Underlying() + 1);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TObjectServiceCacheTest, TestStaleResponse)
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
        TSharedRef::FromString("request"),
        /*suppressUpstreamSync*/ false,
        /*suppressTransactionCoordinatorSync*/ false);

    auto requestId = NRpc::TRequestId::Create();
    auto expirationTime = TDuration::MilliSeconds(10);
    auto data = TSharedRefArray(TSharedRef::FromString("response"));
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
        EXPECT_TRUE(TSharedRefArray::AreBitwiseEqual(data, cookie2.GetValue().Get().Value()->GetResponseMessage()));
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

TEST(TObjectServiceCacheTest, TestStaleError)
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
        TSharedRef::FromString("request"),
        /*suppressUpstreamSync*/ false,
        /*suppressTransactionCoordinatorSync*/ false);

    auto requestId = NRpc::TRequestId::Create();
    auto expirationTime = TDuration::MilliSeconds(10);
    auto data = TSharedRefArray(TSharedRef::FromString("response"));
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
        EXPECT_TRUE(TSharedRefArray::AreBitwiseEqual(data, cookie2.GetValue().Get().Value()->GetResponseMessage()));
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

} // namespace
} // namespace NYT::NObjectClient
