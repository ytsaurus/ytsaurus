#pragma once

#include <yt/server/chunk_pools/sorted_chunk_pool.h>

#include <yt/ytlib/table_client/chunk_slice_fetcher.h>

#include <yt/core/misc/phoenix.h>

#include <contrib/libs/gmock/gmock/gmock.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TMockChunkSliceFetcher
    : public IChunkSliceFetcher
{
public:
    MOCK_METHOD1(AddChunk, void(NChunkClient::TInputChunkPtr));
    MOCK_CONST_METHOD0(GetChunkCount, int());
    MOCK_METHOD0(Fetch, TFuture<void>());
    MOCK_METHOD0(GetChunkSlices, std::vector<NChunkClient::TInputChunkSlicePtr>());
};

typedef TIntrusivePtr<::testing::StrictMock<TMockChunkSliceFetcher>> TStrictMockChunkSliceFetcherPtr;

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

    virtual NTableClient::IChunkSliceFetcherPtr CreateChunkSliceFetcher() override;

    virtual void Persist(const TPersistenceContext& context) override;

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMockChunkSliceFetcherFactory, 0x4fa8873b);

    std::vector<NTableClient::TStrictMockChunkSliceFetcherPtr>* Fetchers_ = nullptr;

    int CurrentIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPool
