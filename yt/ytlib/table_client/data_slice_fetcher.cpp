#include "chunk_slices_fetcher.h"
#include "data_slice_descriptor.h"
#include "data_slice_fetcher.h"

#include <yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/ytlib/chunk_client/input_data_slice.h>

#include <tuple>

namespace NYT {
namespace NTableClient {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TDataSliceFetcher::TDataSliceFetcher(
    TFetcherConfigPtr config,
    i64 chunkSliceSize,
    const TKeyColumns& keyColumns,
    bool sliceByKeys,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr invoker,
    TScrapeChunksCallback scraperCallback,
    NApi::INativeClientPtr client,
    TRowBufferPtr rowBuffer,
    const NLogging::TLogger& logger)
    : ChunkSliceFetcher_(New<TChunkSliceFetcher>(
        std::move(config),
        chunkSliceSize,
        keyColumns,
        sliceByKeys,
        std::move(nodeDirectory),
        std::move(invoker),
        std::move(scraperCallback),
        std::move(client),
        std::move(rowBuffer),
        logger))
{ }

void TDataSliceFetcher::AddChunk(TInputChunkPtr chunk)
{
    ChunkSliceFetcher_->AddChunk(std::move(chunk));
}

TFuture<void> TDataSliceFetcher::Fetch()
{
    return ChunkSliceFetcher_->Fetch();
}

std::vector<TInputDataSlicePtr> TDataSliceFetcher::GetDataSlices()
{
    std::vector<TInputDataSlicePtr> dataSlices;
    auto chunkSlices = ChunkSliceFetcher_->GetChunkSlices();

    std::vector<std::tuple<TKey, bool, int>> boundaries;
    boundaries.reserve(chunkSlices.size() * 2);
    for (int index = 0; index < chunkSlices.size(); ++index) {
        boundaries.emplace_back(chunkSlices[index]->LowerLimit().Key, false, index);
        boundaries.emplace_back(chunkSlices[index]->UpperLimit().Key, true, index);
    }
    std::sort(boundaries.begin(), boundaries.end());
    yhash_set<int> currentChunks;

    int index = 0;
    while (index < boundaries.size()) {
        const auto& boundary = boundaries[index];
        auto currentKey = std::get<0>(boundary);

        while (index < boundaries.size()) {
            const auto& boundary = boundaries[index];
            auto key = std::get<0>(boundary);
            int chunkIndex = std::get<2>(boundary);
            bool isUpper = std::get<1>(boundary);

            if (key != currentKey) {
                break;
            }

            if (isUpper) {
                currentChunks.erase(chunkIndex);
            } else {
                currentChunks.insert(chunkIndex);
            }
            ++index;
        }

        if (!currentChunks.empty()) {
            std::vector<TInputChunkSlicePtr> chunks;
            for (int chunkIndex : currentChunks) {
                chunks.push_back(chunkSlices[chunkIndex]);
            }

            auto upper = index == boundaries.size() ? MaxKey().Get() : std::get<0>(boundaries[index]);

            auto slice = CreateInputDataSlice(
                EDataSliceDescriptorType::VersionedTable,
                std::move(chunks),
                currentKey,
                upper);

            dataSlices.push_back(std::move(slice));
        }
    }

    return dataSlices;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT

