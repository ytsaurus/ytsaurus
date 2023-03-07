#pragma once

#include "public.h"

#include <yt/client/chunk_client/proto/chunk_spec.pb.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TDataSliceDescriptor
{
    std::vector<NProto::TChunkSpec> ChunkSpecs;

    TDataSliceDescriptor() = default;
    explicit TDataSliceDescriptor(std::vector<NProto::TChunkSpec> chunkSpecs);
    explicit TDataSliceDescriptor(NProto::TChunkSpec chunkSpec);

    int GetDataSourceIndex() const;
    int GetRangeIndex() const;
    const NProto::TChunkSpec& GetSingleChunk() const;
    std::optional<i64> GetTag() const;
};

TString ToString(const TDataSliceDescriptor& dataSliceDescriptor);

////////////////////////////////////////////////////////////////////////////////

struct TInterruptDescriptor
{
    std::vector<TDataSliceDescriptor> UnreadDataSliceDescriptors;
    std::vector<TDataSliceDescriptor> ReadDataSliceDescriptors;

    void MergeFrom(TInterruptDescriptor&& source);
};

////////////////////////////////////////////////////////////////////////////////

// Return read limits relative to table (e.g. row index is calculated with addition of table row index).

TReadLimit GetAbsoluteLowerReadLimit(const TDataSliceDescriptor& descriptor, bool versioned);
TReadLimit GetAbsoluteUpperReadLimit(const TDataSliceDescriptor& descriptor, bool versioned);

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    ::google::protobuf::RepeatedPtrField<NProto::TChunkSpec>* chunkSpecs,
    ::google::protobuf::RepeatedField<int>* chunkSpecCountPerDataSlice,
    const std::vector<TDataSliceDescriptor>& dataSlices);

void FromProto(
    std::vector<TDataSliceDescriptor>* dataSlices,
    const ::google::protobuf::RepeatedPtrField<NProto::TChunkSpec>& chunkSpecs,
    const ::google::protobuf::RepeatedField<int>& chunkSpecCountPerDataSlice);

////////////////////////////////////////////////////////////////////////////////

i64 GetCumulativeRowCount(const std::vector<TDataSliceDescriptor>& dataSliceDescriptors);
i64 GetDataSliceDescriptorReaderMemoryEstimate(
    const TDataSliceDescriptor& dataSliceDescriptor,
    TMultiChunkReaderConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
