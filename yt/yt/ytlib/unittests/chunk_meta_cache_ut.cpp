#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_cache.h>
#include <yt/yt/ytlib/chunk_client/config.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

namespace NYT::NChunkClient {
namespace {

using namespace NConcurrency;

using namespace testing;

////////////////////////////////////////////////////////////////////////////////

class TChunkMetaFetcherMock
{
public:
    MOCK_METHOD(TFuture<TRefCountedChunkMetaPtr>, Fetch, (const std::optional<std::vector<int>>&), ());
};

TRefCountedChunkMetaPtr CreateFakeChunkMeta(const std::optional<std::vector<int>>& extensionTags)
{
    auto chunkMeta = New<TRefCountedChunkMeta>();

    if (extensionTags) {
        for (int tag : *extensionTags) {
            auto* ext = chunkMeta->mutable_extensions()->add_extensions();
            ext->set_tag(tag);
            ext->set_data("ChunkMetaExtension_" + ToString(tag));
        }
    }

    return chunkMeta;
}

TFuture<TRefCountedChunkMetaPtr> CreateFakeChunkMetaFuture(const std::optional<std::vector<int>>& extensionTags)
{
    return MakeFuture(CreateFakeChunkMeta(extensionTags));
}

TFuture<TRefCountedChunkMetaPtr> CreateErrorChunkMetaFuture(const std::optional<std::vector<int>>& /*extensionTags*/)
{
    return MakeFuture<TRefCountedChunkMetaPtr>(TError("Test request failure"));
}

////////////////////////////////////////////////////////////////////////////////

TClientChunkMetaCacheConfigPtr CreateCacheConfig(i64 cacheSize)
{
    auto config = New<TClientChunkMetaCacheConfig>();
    // Eviction model is complicated with multiple shards.
    config->Capacity = cacheSize;
    config->ShardCount = 1;

    return config;
}

////////////////////////////////////////////////////////////////////////////////

// For easier providing std::nullopt and std::vector as an EXPECT_CALL argument.
using TTagList = std::optional<std::vector<int>>;

TEST(TCachedChunkMetaTest, Simple)
{
    const auto chunkId = TChunkId(0, 0);
    auto cachedChunkMeta = CreateCachedChunkMeta(chunkId, CreateFakeChunkMeta(std::vector<int>{}));

    TChunkMetaFetcherMock fetcherMock;
    auto fetchFunc = BIND(&TChunkMetaFetcherMock::Fetch, &fetcherMock);

    ON_CALL(fetcherMock, Fetch(_))
        .WillByDefault(Invoke(CreateFakeChunkMetaFuture));

    {
        InSequence sequence;
        EXPECT_CALL(fetcherMock, Fetch(TTagList(std::nullopt)))
            .Times(5);
        EXPECT_CALL(fetcherMock, Fetch(TTagList(std::vector{1, 2, 3})))
            .Times(1);
        EXPECT_CALL(fetcherMock, Fetch(TTagList(std::vector{4, 5})))
            .Times(1);
    }

    for (int index = 0; index < 5; ++index) {
        // Only chunk metas with explicitly specified tags are cached.
        WaitFor(cachedChunkMeta->Fetch(std::nullopt, fetchFunc))
            .ThrowOnError();
    }

    for (int index = 0; index < 5; ++index) {
        WaitFor(cachedChunkMeta->Fetch(std::vector{1, 2, 3}, fetchFunc))
            .ThrowOnError();
    }

    for (int index = 0; index < 5; ++index) {
        WaitFor(cachedChunkMeta->Fetch(std::vector{3, 4, 5}, fetchFunc))
            .ThrowOnError();
    }
}

TEST(TCachedChunkMetaTest, StuckRequests)
{
    const auto chunkId = TChunkId(0, 0);
    auto cachedChunkMeta = CreateCachedChunkMeta(chunkId, CreateFakeChunkMeta(std::vector<int>{}));

    TChunkMetaFetcherMock fetcherMock;
    auto fetchFunc = BIND(&TChunkMetaFetcherMock::Fetch, &fetcherMock);

    auto stuckMeta = NewPromise<TRefCountedChunkMetaPtr>();

    {
        InSequence sequence;
        EXPECT_CALL(fetcherMock, Fetch(TTagList(std::vector{1, 2, 3})))
            .WillOnce(Return(stuckMeta.ToFuture()));
        EXPECT_CALL(fetcherMock, Fetch(TTagList(std::vector{4, 5, 6})))
            .WillOnce(Invoke(CreateFakeChunkMetaFuture));
    }

    std::vector<TFuture<TRefCountedChunkMetaPtr>> stuckRequests;

    for (int index = 0; index < 5; ++index) {
        stuckRequests.emplace_back(cachedChunkMeta->Fetch(std::vector{1, 2, 3}, fetchFunc));
        Yield();
    }

    for (int index = 0; index < 5; ++index) {
        stuckRequests.emplace_back(cachedChunkMeta->Fetch(std::vector{3, 4, 5, 6}, fetchFunc));
        Yield();
    }

    for (int index = 0; index < 5; ++index) {
        WaitFor(cachedChunkMeta->Fetch(std::vector{4, 5, 6}, fetchFunc))
            .ThrowOnError();
    }

    for (const auto& future : stuckRequests) {
        EXPECT_TRUE(!future.IsSet());
    }

    stuckMeta.Set(CreateFakeChunkMeta(std::vector{1, 2, 3}));

    for (const auto& future : stuckRequests) {
        WaitFor(future)
            .ThrowOnError();
    }
}

TEST(TCachedChunkMetaTest, FailedRequests)
{
    const auto chunkId = TChunkId(0, 0);
    auto cachedChunkMeta = CreateCachedChunkMeta(chunkId, CreateFakeChunkMeta(std::vector<int>{}));

    TChunkMetaFetcherMock fetcherMock;
    auto fetchFunc = BIND(&TChunkMetaFetcherMock::Fetch, &fetcherMock);

    {
        InSequence sequence;
        EXPECT_CALL(fetcherMock, Fetch(TTagList(std::vector{1, 2, 3})))
            .Times(5)
            .WillRepeatedly(Invoke(CreateErrorChunkMetaFuture));
        EXPECT_CALL(fetcherMock, Fetch(TTagList(std::vector{1, 2, 3})))
            .Times(1)
            .WillRepeatedly(Invoke(CreateFakeChunkMetaFuture));
    }

    for (int index = 0; index < 5; ++index) {
        auto metaOrError = WaitFor(cachedChunkMeta->Fetch(std::vector{1, 2, 3}, fetchFunc));
        EXPECT_THROW(metaOrError.ThrowOnError(), std::exception);
    }

    for (int index = 0; index < 5; ++index) {
        WaitFor(cachedChunkMeta->Fetch(std::vector{1, 2, 3}, fetchFunc))
            .ThrowOnError();
    }

    for (int index = 0; index < 5; ++index) {
        // Duplicated tags.
        auto metaOrError = WaitFor(cachedChunkMeta->Fetch(std::vector{index, index, index}, fetchFunc));
        EXPECT_THROW(metaOrError.ThrowOnError(), std::exception);
    }
}

////////////////////////////////////////////////////////////////////////////////

TChunkId GenerateBlockChunkId(int seed)
{
    return MakeId(
        NObjectClient::EObjectType::Chunk,
        NObjectClient::InvalidCellTag,
        seed,
        seed);
}

TEST(TClientChunkMetaCacheTest, Simple)
{
    auto config = CreateCacheConfig(10000);
    auto cache = CreateClientChunkMetaCache(config);

    TChunkMetaFetcherMock fetcherMock;
    auto fetchFunc = BIND(&TChunkMetaFetcherMock::Fetch, &fetcherMock);

    {
        InSequence sequence;

        EXPECT_CALL(fetcherMock, Fetch(TTagList(std::vector<int>{})))
            .Times(5)
            .WillRepeatedly(Invoke(CreateFakeChunkMetaFuture));
    }

    for (int index = 0; index < 5; ++index) {
        WaitFor(cache->Fetch(GenerateBlockChunkId(index), std::vector<int>{}, fetchFunc))
            .ValueOrThrow();
    }

    for (int index = 0; index < 5; ++index) {
        WaitFor(cache->Fetch(GenerateBlockChunkId(0), std::vector<int>{}, fetchFunc))
            .ValueOrThrow();
    }
}

TEST(TClientChunkMetaCacheTest, Eviction)
{
    auto config = CreateCacheConfig(1000);
    auto cache = CreateClientChunkMetaCache(config);

    TChunkMetaFetcherMock fetcherMock;
    auto fetchFunc = BIND(&TChunkMetaFetcherMock::Fetch, &fetcherMock);

    ON_CALL(fetcherMock, Fetch(_))
        .WillByDefault(Invoke(CreateFakeChunkMetaFuture));

    std::vector<int> hugeTagList(100);
    // 0, 1, 2, .., 99
    std::iota(hugeTagList.begin(), hugeTagList.end(), 0);

    {
        InSequence sequence;

        EXPECT_CALL(fetcherMock, Fetch(TTagList(std::vector{1,})))
            .Times(100);

        // 100 items is more than enought to overflow cache capacity, so second pass is not cached.
        EXPECT_CALL(fetcherMock, Fetch(TTagList(std::vector{1,})))
            .Times(100);

        EXPECT_CALL(fetcherMock, Fetch(TTagList(hugeTagList)))
            .Times(5);
    }

    for (int index = 0; index < 100; ++index) {
        WaitFor(cache->Fetch(GenerateBlockChunkId(index), std::vector<int>{1,}, fetchFunc))
            .ValueOrThrow();
    }
    for (int index = 0; index < 100; ++index) {
        WaitFor(cache->Fetch(GenerateBlockChunkId(index), std::vector<int>{1,}, fetchFunc))
            .ValueOrThrow();
    }

    // A few last items should be cached.
    for (int index = 95; index < 100; ++index) {
        WaitFor(cache->Fetch(GenerateBlockChunkId(index), std::vector<int>{1,}, fetchFunc))
            .ValueOrThrow();
    }

    // Item which does not fit in cache.
    for (int index = 0; index < 5; ++index) {
        WaitFor(cache->Fetch(GenerateBlockChunkId(index), hugeTagList, fetchFunc))
            .ValueOrThrow();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkClient
