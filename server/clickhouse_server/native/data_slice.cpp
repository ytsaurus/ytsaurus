#include "data_slice.h"

#include <yt/ytlib/chunk_client/chunk_spec.h>
#include <yt/ytlib/chunk_client/data_slice_descriptor.h>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

std::vector<TDataSliceDescriptorList> SplitUnversionedChunks(
    TChunkSpecList chunkSpecs,
    size_t maxTableParts)
{
    std::vector<TDataSliceDescriptorList> result;

    auto createJob = [&] (TChunkSpecList inputChunks) {
        // for unversioned tables split is very simple: one chunk per slice
        TDataSliceDescriptorList dataSliceDescriptors;
        for (auto& chunkSpec: inputChunks) {
            dataSliceDescriptors.emplace_back(std::move(chunkSpec));
        }

        result.emplace_back(std::move(dataSliceDescriptors));
    };

    // TODO(max42): rework.
    if (maxTableParts > 1) {
        ui64 totalDataWeight = 0;
        for (const auto& chunkSpec: chunkSpecs) {
            totalDataWeight += chunkSpec.data_weight_override();
        }

        ui64 currentDataWeight = 0;
        TChunkSpecList currentChunkSpecs;

        for (auto& chunkSpec: chunkSpecs) {
            currentDataWeight += chunkSpec.data_weight_override();
            currentChunkSpecs.emplace_back(std::move(chunkSpec));

            if (currentDataWeight > totalDataWeight / maxTableParts) {
                currentDataWeight = 0;
                createJob(std::move(currentChunkSpecs));
            }
        }

        if (!currentChunkSpecs.empty()) {
            createJob(std::move(currentChunkSpecs));
        }
    } else {
        createJob(std::move(chunkSpecs));
    }

    return result;
}

std::vector<TDataSliceDescriptorList> SplitVersionedChunks(
    TChunkSpecList chunkSpecs,
    size_t maxTableParts)
{
    // TODO
    Y_UNUSED(chunkSpecs);
    Y_UNUSED(maxTableParts);

    THROW_ERROR_EXCEPTION("Versioned tables not supported");
}

std::vector<TDataSliceDescriptorList> MergeUnversionedChunks(
    TDataSliceDescriptorList dataSliceDescriptors,
    size_t maxTableParts)
{
    std::vector<TDataSliceDescriptorList> result;

    if (maxTableParts > 1) {
        TChunkSpecList chunkSpecs;
        for (auto& dataSlice: dataSliceDescriptors) {
            for (auto& chunkSpec: dataSlice.ChunkSpecs) {
                chunkSpecs.emplace_back(std::move(chunkSpec));
            }
        }

        result = SplitUnversionedChunks(
            std::move(chunkSpecs),
            maxTableParts);
    } else {
        result.emplace_back(std::move(dataSliceDescriptors));
    }

    return result;
}

std::vector<TDataSliceDescriptorList> MergeVersionedChunks(
    TDataSliceDescriptorList dataSliceDescriptors,
    size_t maxTableParts)
{
    // TODO
    Y_UNUSED(dataSliceDescriptors);
    Y_UNUSED(maxTableParts);

    THROW_ERROR_EXCEPTION("Versioned tables not supported");
}

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
