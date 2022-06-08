#include "client_impl.h"

#include <yt/yt/ytlib/chunk_client/chunk_fragment_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/ytlib/node_tracker_client/node_status_directory.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

std::vector<TSharedRef> TClient::DoReadHunks(
    const std::vector<TReadHunkRequest>& requests,
    const TReadHunksOptions& options)
{
    auto reader = CreateChunkFragmentReader(
        options.Config,
        this,
        CreateTrivialNodeStatusDirectory(),
        /*profiler*/ {});

    std::vector<IChunkFragmentReader::TChunkFragmentRequest> readerRequests;
    readerRequests.reserve(requests.size());
    for (const auto& request : requests) {
        readerRequests.push_back(IChunkFragmentReader::TChunkFragmentRequest{
            .ChunkId = request.ChunkId,
            .ErasureCodec = request.ErasureCodec,
            .Length = request.Length,
            .BlockIndex = request.BlockIndex,
            .BlockOffset = request.BlockOffset,
            .BlockSize = request.BlockSize
        });
    }

    auto response = WaitFor(reader->ReadFragments(/*options*/ {}, readerRequests))
        .ValueOrThrow();

    return response.Fragments;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

