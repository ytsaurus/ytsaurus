#include "data_slice_descriptor.h"
#include "chunk_spec.h"
#include "helpers.h"

#include <yt/client/chunk_client/read_limit.h>

namespace NYT::NChunkClient {

using namespace NTableClient;
using namespace NTransactionClient;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TDataSliceDescriptor::TDataSliceDescriptor(
    std::vector<NProto::TChunkSpec> chunkSpecs)
    : ChunkSpecs(std::move(chunkSpecs))
{ }

TDataSliceDescriptor::TDataSliceDescriptor(
    NProto::TChunkSpec chunkSpec)
{
    ChunkSpecs.push_back(std::move(chunkSpec));
}

const NProto::TChunkSpec& TDataSliceDescriptor::GetSingleChunk() const
{
    YT_VERIFY(ChunkSpecs.size() == 1);
    return ChunkSpecs[0];
}

std::optional<i64> TDataSliceDescriptor::GetTag() const
{
    YT_VERIFY(!ChunkSpecs.empty());
    std::optional<i64> commonTag = ChunkSpecs.front().has_data_slice_tag()
        ? std::make_optional(ChunkSpecs.front().data_slice_tag())
        : std::nullopt;
    for (const auto& chunkSpec : ChunkSpecs) {
        std::optional<i64> tag = chunkSpec.has_data_slice_tag()
            ? std::make_optional(chunkSpec.data_slice_tag())
            : std::nullopt;
        YT_VERIFY(commonTag == tag);
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

TString ToString(const TDataSliceDescriptor& dataSliceDescriptor)
{
    TStringBuilder stringBuilder;
    stringBuilder.AppendChar('{');
    bool isFirst = true;
    for (const auto& chunkSpec : dataSliceDescriptor.ChunkSpecs) {
        if (!isFirst) {
            stringBuilder.AppendString(", ");
        }
        stringBuilder.AppendString(ToString(FromProto<TChunkId>(chunkSpec.chunk_id())));
        if (chunkSpec.has_lower_limit() || chunkSpec.has_upper_limit()) {
            stringBuilder.AppendChar('[');
            if (chunkSpec.has_lower_limit()) {
                stringBuilder.AppendString(ToString(FromProto<TReadLimit>(chunkSpec.lower_limit())));
            }
            stringBuilder.AppendChar(':');
            if (chunkSpec.has_upper_limit()) {
                stringBuilder.AppendString(ToString(FromProto<TReadLimit>(chunkSpec.upper_limit())));
            }
            stringBuilder.AppendChar(']');
        }
        isFirst = false;
    }
    stringBuilder.AppendChar('}');
    return stringBuilder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

TReadLimit GetAbsoluteLowerReadLimit(const TDataSliceDescriptor& descriptor, bool versioned)
{
    TReadLimit result;

    if (versioned) {
        for (const auto& chunkSpec : descriptor.ChunkSpecs) {
            TReadLimit readLimit;
            FromProto(&readLimit, chunkSpec.lower_limit());
            YT_VERIFY(!readLimit.HasRowIndex());

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
            YT_VERIFY(!readLimit.HasRowIndex());

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

} // namespace NYT::NChunkClient
