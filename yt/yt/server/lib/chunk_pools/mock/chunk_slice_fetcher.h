#pragma once

#include <yt/yt/server/lib/chunk_pools/sorted_chunk_pool.h>

#include <yt/yt/ytlib/table_client/chunk_slice_fetcher.h>

#include <yt/yt/core/misc/phoenix.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TMockChunkSliceFetcher
    : public IChunkSliceFetcher
{
public:
    MOCK_METHOD(void, AddChunk, (NChunkClient::TInputChunkPtr), (override));
    MOCK_METHOD(void, AddDataSliceForSlicing, (NChunkClient::TLegacyDataSlicePtr, const TComparator& comparator, i64, bool), (override));
    MOCK_METHOD(int, GetChunkCount, (), (const, override));
    MOCK_METHOD(TFuture<void>, Fetch, (), (override));
    MOCK_METHOD(std::vector<NChunkClient::TInputChunkSlicePtr>, GetChunkSlices, (), (override));
    MOCK_METHOD(void, SetCancelableContext, (TCancelableContextPtr), (override));
};

using TStrictMockChunkSliceFetcherPtr = TIntrusivePtr<::testing::StrictMock<TMockChunkSliceFetcher>>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

class TMockChunkSliceFetcherFactory
    : public IChunkSliceFetcherFactory
{
public:
    //! Used only for persistence.
    TMockChunkSliceFetcherFactory() = default;

    TMockChunkSliceFetcherFactory(std::vector<NTableClient::TStrictMockChunkSliceFetcherPtr>* fetchers);

    NTableClient::IChunkSliceFetcherPtr CreateChunkSliceFetcher() override;

    void Persist(const TPersistenceContext& context) override;

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMockChunkSliceFetcherFactory, 0x4fa8873b);

    std::vector<NTableClient::TStrictMockChunkSliceFetcherPtr>* Fetchers_ = nullptr;

    int CurrentIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
