#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/public.h>

#include <vector>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

using TChunkSpecList = std::vector<NChunkClient::NProto::TChunkSpec>;
using TDataSliceDescriptorList = std::vector<NChunkClient::TDataSliceDescriptor>;

////////////////////////////////////////////////////////////////////////////////

std::vector<TDataSliceDescriptorList> SplitUnversionedChunks(
    TChunkSpecList chunkSpecs,
    size_t maxTableParts);

std::vector<TDataSliceDescriptorList> SplitVersionedChunks(
    TChunkSpecList chunkSpecs,
    size_t maxTableParts);

std::vector<TDataSliceDescriptorList> MergeUnversionedChunks(
    TDataSliceDescriptorList dataSliceDescriptors,
    size_t maxTableParts);

std::vector<TDataSliceDescriptorList> MergeVersionedChunks(
    TDataSliceDescriptorList dataSliceDescriptors,
    size_t maxTableParts);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
