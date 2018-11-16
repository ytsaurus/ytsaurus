#include "data_slice_descriptor.h"
#include "chunk_spec.h"
#include "helpers.h"

#include <yt/client/chunk_client/read_limit.h>

namespace NYT {
namespace NChunkClient {

using namespace NTableClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TDataSliceDescriptor::TDataSliceDescriptor(std::vector<NProto::TChunkSpec> chunkSpecs)
    : ChunkSpecs(std::move(chunkSpecs))
{ }

TDataSliceDescriptor::TDataSliceDescriptor(const NProto::TChunkSpec& chunkSpec)
{
    ChunkSpecs.push_back(chunkSpec);
}

const NProto::TChunkSpec& TDataSliceDescriptor::GetSingleChunk() const
{
    YCHECK(ChunkSpecs.size() == 1);
    return ChunkSpecs[0];
}

TNullable<i64> TDataSliceDescriptor::GetTag() const
{
    YCHECK(!ChunkSpecs.empty());
    TNullable<i64> commonTag = ChunkSpecs.front().has_data_slice_tag()
        ? MakeNullable(ChunkSpecs.front().data_slice_tag())
        : Null;
    for (const auto& chunkSpec : ChunkSpecs) {
        TNullable<i64> tag = chunkSpec.has_data_slice_tag()
            ? MakeNullable(chunkSpec.data_slice_tag())
            : Null;
        YCHECK(commonTag == tag);
    }
    return commonTag;
}

int TDataSliceDescriptor::GetDataSourceIndex() const
{
    return ChunkSpecs.empty()
        ? 0
        : ChunkSpecs.front().table_index();
}

int TDataSliceDescriptor::GetRangeIndex() const
{
    return ChunkSpecs.empty()
       ? 0
       : ChunkSpecs.front().range_index();
}

////////////////////////////////////////////////////////////////////////////////

TReadLimit GetAbsoluteLowerReadLimit(const TDataSliceDescriptor& descriptor, bool versioned)
{
    TReadLimit result;

    if (versioned) {
        for (const auto& chunkSpec : descriptor.ChunkSpecs) {
            TReadLimit readLimit;
            FromProto(&readLimit, chunkSpec.lower_limit());
            YCHECK(!readLimit.HasRowIndex());

            if (readLimit.HasKey() && (!result.HasKey() || result.GetKey() > readLimit.GetKey())) {
                result.SetKey(readLimit.GetKey());
            }
        }
    } else {
        const auto& chunkSpec = descriptor.GetSingleChunk();
        TReadLimit readLimit;
        FromProto(&readLimit, chunkSpec.lower_limit());
        if (readLimit.HasRowIndex()) {
            result.SetRowIndex(readLimit.GetRowIndex() + chunkSpec.table_row_index());
        } else {
            result.SetRowIndex(chunkSpec.table_row_index());
        }

        if (readLimit.HasKey()) {
            result.SetKey(readLimit.GetKey());
        };
    }

    return result;
}

TReadLimit GetAbsoluteUpperReadLimit(const TDataSliceDescriptor& descriptor, bool versioned)
{
    TReadLimit result;

    if (versioned) {
        for (const auto& chunkSpec : descriptor.ChunkSpecs) {
            TReadLimit readLimit;
            FromProto(&readLimit, chunkSpec.upper_limit());
            YCHECK(!readLimit.HasRowIndex());

            if (readLimit.HasKey() && (!result.HasKey() || result.GetKey() < readLimit.GetKey())) {
                result.SetKey(readLimit.GetKey());
            }
        }
    } else {
        const auto& chunkSpec = descriptor.GetSingleChunk();
        TReadLimit readLimit;
        FromProto(&readLimit, chunkSpec.upper_limit());
        if (readLimit.HasRowIndex()) {
            result.SetRowIndex(readLimit.GetRowIndex() + chunkSpec.table_row_index());
        } else {
            result.SetRowIndex(chunkSpec.table_row_index() + chunkSpec.row_count_override());
        }

        if (readLimit.HasKey()) {
            result.SetKey(readLimit.GetKey());
        };
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TInterruptDescriptor::MergeFrom(TInterruptDescriptor&& source)
{
    std::move(
        source.ReadDataSliceDescriptors.begin(),
        source.ReadDataSliceDescriptors.end(),
        std::back_inserter(ReadDataSliceDescriptors));
    source.ReadDataSliceDescriptors.clear();
    std::move(
        source.UnreadDataSliceDescriptors.begin(),
        source.UnreadDataSliceDescriptors.end(),
        std::back_inserter(UnreadDataSliceDescriptors));
    source.UnreadDataSliceDescriptors.clear();
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TDataSliceDescriptor* protoDataSliceDescriptor, const TDataSliceDescriptor& dataSliceDescriptor)
{
    for (const auto& chunkSpec : dataSliceDescriptor.ChunkSpecs) {
        *protoDataSliceDescriptor->add_chunks() = chunkSpec;
    }
}

void FromProto(TDataSliceDescriptor* dataSliceDescriptor, const NProto::TDataSliceDescriptor& protoDataSliceDescriptor)
{
    dataSliceDescriptor->ChunkSpecs = std::vector<NProto::TChunkSpec>(protoDataSliceDescriptor.chunks().begin(), protoDataSliceDescriptor.chunks().end());
}

void ToProto(
    ::google::protobuf::RepeatedPtrField<NProto::TChunkSpec>* chunkSpecs,
    ::google::protobuf::RepeatedField<int>* chunkSpecCountPerDataSlice,
    const std::vector<TDataSliceDescriptor>& dataSlices)
{
    for (const auto& dataSlice : dataSlices) {
        chunkSpecCountPerDataSlice->Add(dataSlice.ChunkSpecs.size());

        for (const auto& chunkSpec : dataSlice.ChunkSpecs) {
            *chunkSpecs->Add() = chunkSpec;
        }
    }
}

void FromProto(
    std::vector<TDataSliceDescriptor>* dataSlices,
    const ::google::protobuf::RepeatedPtrField<NProto::TChunkSpec>& chunkSpecs,
    const ::google::protobuf::RepeatedField<int>& chunkSpecCountPerDataSlice)
{
    dataSlices->clear();
    int currentIndex = 0;
    for (int chunkSpecCount : chunkSpecCountPerDataSlice) {
        std::vector<NProto::TChunkSpec> dataSliceSpecs(
            chunkSpecs.begin() + currentIndex,
            chunkSpecs.begin() + currentIndex + chunkSpecCount);

        dataSlices->emplace_back(std::move(dataSliceSpecs));
        currentIndex += chunkSpecCount;
    }
}

////////////////////////////////////////////////////////////////////////////////

i64 GetCumulativeRowCount(const std::vector<TDataSliceDescriptor>& dataSliceDescriptors)
{
    i64 result = 0;
    for (const auto& dataSliceDescriptor : dataSliceDescriptors) {
        result += GetCumulativeRowCount(dataSliceDescriptor.ChunkSpecs);
    }
    return result;
}

i64 GetDataSliceDescriptorReaderMemoryEstimate(const TDataSliceDescriptor& dataSliceDescriptor, TMultiChunkReaderConfigPtr config)
{
    i64 result = 0;
    for (const auto& chunkSpec : dataSliceDescriptor.ChunkSpecs) {
        result += GetChunkReaderMemoryEstimate(chunkSpec, config);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
