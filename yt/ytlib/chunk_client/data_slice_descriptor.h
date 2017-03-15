#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/data_slice_descriptor.pb.h>

#include <yt/ytlib/table_client/schema.h>

#include <yt/ytlib/transaction_client/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TDataSliceDescriptor
{
    std::vector<NProto::TChunkSpec> ChunkSpecs;

    TDataSliceDescriptor() = default;
    explicit TDataSliceDescriptor(std::vector<NProto::TChunkSpec> chunkSpecs);
    TDataSliceDescriptor(const NProto::TChunkSpec& chunkSpec);

    int GetDataSourceIndex() const;

    const NProto::TChunkSpec& GetSingleChunk() const;

    TNullable<i64> GetCommonTag() const;
};

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TDataSliceDescriptor* protoDataSliceDescriptor,
    const TDataSliceDescriptor& dataSliceDescriptor);
void FromProto(
    TDataSliceDescriptor* dataSliceDescriptor,
    const NProto::TDataSliceDescriptor& protoDataSliceDescriptor);

////////////////////////////////////////////////////////////////////////////////

i64 GetCumulativeRowCount(const std::vector<TDataSliceDescriptor>& dataSliceDescriptors);
i64 GetDataSliceDescriptorReaderMemoryEstimate(
    const TDataSliceDescriptor& dataSliceDescriptor,
    TMultiChunkReaderConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
