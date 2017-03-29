#pragma once

#include <yt/ytlib/table_client/chunk_slice_fetcher.h>

#include <contrib/libs/gmock/gmock/gmock.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TMockChunkSliceFetcher
    : public IChunkSliceFetcher
{
public:
    MOCK_METHOD1(AddChunk, void(NChunkClient::TInputChunkPtr));
    MOCK_METHOD0(Fetch, TFuture<void>());
    MOCK_METHOD0(GetChunkSlices, std::vector<NChunkClient::TInputChunkSlicePtr>());
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
