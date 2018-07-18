#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/data_slice_descriptor.pb.h>

#include <yt/client/table_client/schema.h>

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

    TNullable<i64> GetTag() const;
};

////////////////////////////////////////////////////////////////////////////////

// COMPAT(psushin)
const TDataSliceDescriptor& GetIncompatibleDataSliceDescriptor();

////////////////////////////////////////////////////////////////////////////////

struct TInterruptDescriptor
{
    std::vector<TDataSliceDescriptor> UnreadDataSliceDescriptors;
    std::vector<TDataSliceDescriptor> ReadDataSliceDescriptors;

    void MergeFrom(TInterruptDescriptor&& source);
};

////////////////////////////////////////////////////////////////////////////////

void MergeInterruptDescriptors(TInterruptDescriptor* source, TInterruptDescriptor&& target);

////////////////////////////////////////////////////////////////////////////////

// Deprecated.
void ToProto(
    NProto::TDataSliceDescriptor* protoDataSliceDescriptor,
    const TDataSliceDescriptor& dataSliceDescriptor);
void FromProto(
    TDataSliceDescriptor* dataSliceDescriptor,
    const NProto::TDataSliceDescriptor& protoDataSliceDescriptor);

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

} // namespace NChunkClient
} // namespace NYT
