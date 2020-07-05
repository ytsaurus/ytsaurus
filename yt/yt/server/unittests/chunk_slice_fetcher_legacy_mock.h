#pragma once

// For TMockChunkSliceFetcher.
#include "chunk_slice_fetcher_mock.h"

#include <yt/server/lib/legacy_chunk_pools/sorted_chunk_pool.h>

#include <yt/ytlib/table_client/chunk_slice_fetcher.h>

#include <yt/core/misc/phoenix.h>

#include <yt/core/actions/cancelable_context.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NLegacyChunkPools {

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
    DECLARE_DYNAMIC_PHOENIX_TYPE(TMockChunkSliceFetcherFactory, 0x4fa8873c);

    std::vector<NTableClient::TStrictMockChunkSliceFetcherPtr>* Fetchers_ = nullptr;

    int CurrentIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLegacyChunkPools
