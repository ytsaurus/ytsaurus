#pragma once

#include "public.h"

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TDataSliceDescriptor
{
    std::vector<NProto::TChunkSpec> ChunkSpecs;
    //! Index of a row in virtual column directory.
    // TODO(max42): ToProto/FromProto and introduce data slice descriptor extension.
    std::optional<i64> VirtualRowIndex;

    TDataSliceDescriptor() = default;
    explicit TDataSliceDescriptor(std::vector<NProto::TChunkSpec> chunkSpecs);
    explicit TDataSliceDescriptor(
        NProto::TChunkSpec chunkSpec,
        std::optional<i64> virtualRowIndex = std::nullopt);

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

TLegacyReadLimit GetAbsoluteReadLimit(const TDataSliceDescriptor& descriptor, bool isUpper, bool versioned, bool sorted);
TLegacyReadLimit GetAbsoluteLowerReadLimit(const TDataSliceDescriptor& descriptor, bool versioned, bool sorted);
TLegacyReadLimit GetAbsoluteUpperReadLimit(const TDataSliceDescriptor& descriptor, bool versioned, bool sorted);

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): replace with function accepting NProto::TTableInputSpec* and
// possibly unify with TTask::AddChunksToInputSpec.
void ToProto(
    ::google::protobuf::RepeatedPtrField<NProto::TChunkSpec>* chunkSpecs,
    ::google::protobuf::RepeatedField<int>* chunkSpecCountPerDataSlice,
    ::google::protobuf::RepeatedField<i64>* virtualRowIndexPerDataSlice,
    const std::vector<TDataSliceDescriptor>& dataSlices);

void FromProto(
    std::vector<TDataSliceDescriptor>* dataSlices,
    const ::google::protobuf::RepeatedPtrField<NProto::TChunkSpec>& chunkSpecs,
    const ::google::protobuf::RepeatedField<int>& chunkSpecCountPerDataSlice,
    const ::google::protobuf::RepeatedField<i64>& virtualRowIndexPerDataSlice);

////////////////////////////////////////////////////////////////////////////////

i64 GetCumulativeRowCount(const std::vector<TDataSliceDescriptor>& dataSliceDescriptors);
i64 GetDataSliceDescriptorReaderMemoryEstimate(
    const TDataSliceDescriptor& dataSliceDescriptor,
    TMultiChunkReaderConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
