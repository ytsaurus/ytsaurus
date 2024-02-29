#include "data_slice_descriptor.h"
#include "chunk_spec.h"
#include "helpers.h"

#include <yt/yt/client/chunk_client/read_limit.h>

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
    NProto::TChunkSpec chunkSpec,
    std::optional<i64> virtualRowIndex)
    : ChunkSpecs{std::move(chunkSpec)}
    , VirtualRowIndex(virtualRowIndex)
{ }

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
                stringBuilder.AppendString(ToString(FromProto<TLegacyReadLimit>(chunkSpec.lower_limit())));
            }
            stringBuilder.AppendChar(':');
            if (chunkSpec.has_upper_limit()) {
                stringBuilder.AppendString(ToString(FromProto<TLegacyReadLimit>(chunkSpec.upper_limit())));
            }
            stringBuilder.AppendChar(']');
        }
        isFirst = false;
    }
    stringBuilder.AppendChar('}');
    return stringBuilder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

TLegacyReadLimit GetAbsoluteReadLimit(const TDataSliceDescriptor& descriptor, bool isUpper, bool versioned, bool sorted)
{
    TLegacyReadLimit result;

    if (versioned && sorted) {
        auto needUpdate = [&] (const TLegacyOwningKey& legacyKey) {
            if (!result.HasLegacyKey()) {
                return true;
            }
            if (isUpper) {
                return result.GetLegacyKey() < legacyKey;
            } else {
                return result.GetLegacyKey() > legacyKey;
            }
        };

        for (const auto& chunkSpec : descriptor.ChunkSpecs) {
            TLegacyReadLimit readLimit;
            FromProto(&readLimit, isUpper ? chunkSpec.upper_limit() : chunkSpec.lower_limit());
            YT_VERIFY(!readLimit.HasRowIndex());

            if (readLimit.HasLegacyKey() && needUpdate(readLimit.GetLegacyKey())) {
                result.SetLegacyKey(readLimit.GetLegacyKey());
            }
        }
    } else {
        const auto& chunkSpec = descriptor.GetSingleChunk();
        TLegacyReadLimit readLimit;
        FromProto(&readLimit, isUpper ? chunkSpec.upper_limit() : chunkSpec.lower_limit());
        if (readLimit.HasRowIndex()) {
            result.SetRowIndex(readLimit.GetRowIndex() + chunkSpec.table_row_index());
        } else {
            result.SetRowIndex(chunkSpec.table_row_index() + isUpper ? chunkSpec.row_count_override() : 0);
        }

        if (versioned) {
            YT_VERIFY(chunkSpec.has_tablet_index());
            result.SetTabletIndex(chunkSpec.tablet_index());
        }

        if (readLimit.HasLegacyKey()) {
            result.SetLegacyKey(readLimit.GetLegacyKey());
        }
    }

    return result;
}

TLegacyReadLimit GetAbsoluteLowerReadLimit(const TDataSliceDescriptor& descriptor, bool versioned, bool sorted)
{
    return GetAbsoluteReadLimit(descriptor, /*isUpper*/ false, versioned, sorted);
}

TLegacyReadLimit GetAbsoluteUpperReadLimit(const TDataSliceDescriptor& descriptor, bool versioned, bool sorted)
{
    return GetAbsoluteReadLimit(descriptor, /*isUpper*/ true, versioned, sorted);
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
    ::google::protobuf::RepeatedField<i64>* virtualRowIndexPerDataSlice,
    const std::vector<TDataSliceDescriptor>& dataSlices)
{
    for (const auto& dataSlice : dataSlices) {
        chunkSpecCountPerDataSlice->Add(dataSlice.ChunkSpecs.size());
        virtualRowIndexPerDataSlice->Add(dataSlice.VirtualRowIndex.value_or(-1));
        for (const auto& chunkSpec : dataSlice.ChunkSpecs) {
            *chunkSpecs->Add() = chunkSpec;
        }
    }
}

void FromProto(
    std::vector<TDataSliceDescriptor>* dataSlices,
    const ::google::protobuf::RepeatedPtrField<NProto::TChunkSpec>& chunkSpecs,
    const ::google::protobuf::RepeatedField<int>& chunkSpecCountPerDataSlice,
    const ::google::protobuf::RepeatedField<i64>& virtualRowIndexPerDataSlice)
{
    dataSlices->clear();
    int chunkSpecIndex = 0;
    for (int dataSliceIndex = 0; dataSliceIndex < chunkSpecCountPerDataSlice.size(); ++dataSliceIndex) {
        int chunkSpecCount = chunkSpecCountPerDataSlice[dataSliceIndex];

        std::optional<i64> virtualRowIndex;
        if (dataSliceIndex >= virtualRowIndexPerDataSlice.size()) {
            // XXX(max42): why?
            virtualRowIndex = std::nullopt;
        } else if (virtualRowIndexPerDataSlice[dataSliceIndex] == -1) {
            // -1 stands for nullopt.
            virtualRowIndex = std::nullopt;
        } else {
            virtualRowIndex = virtualRowIndexPerDataSlice[dataSliceIndex];
        }

        std::vector<NProto::TChunkSpec> dataSliceChunkSpecs(
            chunkSpecs.begin() + chunkSpecIndex,
            chunkSpecs.begin() + chunkSpecIndex + chunkSpecCount);
        auto& dataSlice = dataSlices->emplace_back(std::move(dataSliceChunkSpecs));
        chunkSpecIndex += chunkSpecCount;
        dataSlice.VirtualRowIndex = virtualRowIndex;
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
