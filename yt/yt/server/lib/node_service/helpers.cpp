#include "helpers.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/table_client/chunk_slice.h>
#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/table_client/key_set.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NDataNode {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NRpc;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TError ProcessSliceChunk(
    const TSliceRequest& sliceRequest,
    TRspGetChunkSlices::TSliceResponse* sliceResponse,
    const TKeySetWriterPtr& keysWriter,
    const TKeySetWriterPtr& keyBoundsWriter,
    const TErrorOr<TRefCountedChunkMetaPtr>& metaOrError)
{
    auto chunkId = FromProto<TChunkId>(sliceRequest.chunk_id());
    try {
        THROW_ERROR_EXCEPTION_IF_FAILED(metaOrError, "Error getting meta of chunk %v",
            chunkId);

        const auto& chunkMeta = metaOrError.Value();
        auto type = CheckedEnumCast<EChunkType>(chunkMeta->type());
        if (type != EChunkType::Table) {
            THROW_ERROR_EXCEPTION("Invalid type of chunk %v: expected %Qlv, actual %Qlv",
                chunkId,
                EChunkType::Table,
                type);
        }

        auto miscExt = GetProtoExtension<TMiscExt>(chunkMeta->extensions());
        if (!miscExt.sorted()) {
            THROW_ERROR_EXCEPTION("Chunk %v is not sorted", chunkId);
        }

        auto slices = SliceChunk(
            sliceRequest,
            *chunkMeta);

        for (const auto& slice : slices) {
            ToProto(keysWriter, keyBoundsWriter, sliceResponse->add_chunk_slices(), slice);
        }
    } catch (const std::exception& ex) {
        auto error = TError(ex);
        ToProto(sliceResponse->mutable_error(), error);
        return error;
    }

    return TError();
}

std::vector<TError> ProcessGetChunkSlicesRequest(
    const TReqGetChunkSlices& request,
    TTypedServiceResponse<TRspGetChunkSlices>& response,
    int requestCount,
    const std::vector<TErrorOr<TRefCountedChunkMetaPtr>>& chunkMetas)
{
    std::vector<TError> errors;

    auto keysWriter = New<TKeySetWriter>();
    auto keyBoundsWriter = New<TKeySetWriter>();
    for (int requestIndex = 0; requestIndex < requestCount; ++requestIndex) {
        const auto& sliceRequest = request.slice_requests(requestIndex);

        auto error = ProcessSliceChunk(
            sliceRequest,
            response.add_slice_responses(),
            keysWriter,
            keyBoundsWriter,
            chunkMetas[requestIndex]);
        if (!error.IsOK()) {
            auto chunkId = FromProto<TChunkId>(sliceRequest.chunk_id());
            error = error
                << TErrorAttribute("chunk_id", chunkId);
            errors.push_back(std::move(error));
        }
    }

    response.Attachments().push_back(keysWriter->Finish());
    response.Attachments().push_back(keyBoundsWriter->Finish());

    return errors;
}

////////////////////////////////////////////////////////////////////////////////

void SerializeSample(
    NChunkClient::NProto::TRspGetTableSamples::TSample* protoSample,
    TUnversionedValueRange values,
    i32 maxSampleSize,
    i64 weight,
    const TKeySetWriterPtr& keySetWriter,
    const TRowBufferPtr& truncatedSampleValueBuffer)
{
    auto truncatedValues = TruncateUnversionedValues(values, truncatedSampleValueBuffer, {.ClipAfterOverflow = true, .MaxTotalSize = maxSampleSize});

    protoSample->set_key_index(keySetWriter->WriteValueRange(truncatedValues.Values));
    protoSample->set_incomplete(truncatedValues.Clipped);
    protoSample->set_weight(weight);
}

TError ProcessSortingSamples(
    const TReqGetTableSamples::TSampleRequest& sampleRequest,
    TRspGetTableSamples::TChunkSamples* chunkSamples,
    const TKeyColumns& keyColumns,
    i32 maxSampleSize,
    const TKeySetWriterPtr& keySetWriter,
    const TChunkMeta& chunkMeta)
{
    TNameTablePtr nameTable;
    std::vector<int> keyIds;

    try {
        auto nameTableExt = FindProtoExtension<TNameTableExt>(chunkMeta.extensions());
        if (nameTableExt) {
            nameTable = FromProto<TNameTablePtr>(*nameTableExt);
        } else {
            auto schemaExt = GetProtoExtension<TTableSchemaExt>(chunkMeta.extensions());
            nameTable = TNameTable::FromSchema(FromProto<TTableSchema>(schemaExt));
        }

        for (const auto& column : keyColumns) {
            keyIds.push_back(nameTable->GetIdOrRegisterName(column));
        }
    } catch (const std::exception& ex) {
        auto chunkId = FromProto<TChunkId>(sampleRequest.chunk_id());
        auto error = TError(ex).Wrap("Failed to gather chunk samples (ChunkId: %v)", chunkId);

        // We failed to deserialize name table, so we don't return any samples.
        return error;
    }

    std::vector<int> idToKeyIndex(nameTable->GetSize(), -1);
    for (int i = 0; i < std::ssize(keyIds); ++i) {
        idToKeyIndex[keyIds[i]] = i;
    }

    auto samplesExt = GetProtoExtension<TSamplesExt>(chunkMeta.extensions());

    auto truncatedSampleValueBuffer = New<TRowBuffer>();

    // TODO(psushin): respect sampleRequest lower_limit and upper_limit.
    // Old chunks do not store samples weights.
    bool hasWeights = samplesExt.weights_size() > 0;
    for (int index = 0;
        index < samplesExt.entries_size() && chunkSamples->samples_size() < sampleRequest.sample_count();
        ++index)
    {
        int remaining = samplesExt.entries_size() - index;
        if (std::rand() % remaining >= sampleRequest.sample_count() - chunkSamples->samples_size()) {
            continue;
        }

        auto row = FromProto<TUnversionedOwningRow>(samplesExt.entries(index));
        std::vector<TUnversionedValue> values(
            keyColumns.size(),
            MakeUnversionedSentinelValue(EValueType::Null));

        for (const auto& value : row) {
            int keyIndex = idToKeyIndex[value.Id];
            if (keyIndex < 0) {
                continue;
            }
            values[keyIndex] = value;
        }

        SerializeSample(
            chunkSamples->add_samples(),
            values,
            maxSampleSize,
            hasWeights ? samplesExt.weights(index) : samplesExt.entries(index).length(),
            keySetWriter,
            truncatedSampleValueBuffer);
    }

    return TError();
}

TError ProcessPartitioningSamples(
    const TReqGetTableSamples::TSampleRequest& sampleRequest,
    TRspGetTableSamples::TChunkSamples* chunkSamples,
    const TKeyColumns& keyColumns,
    const TKeySetWriterPtr& keySetWriter,
    const TChunkMeta& chunkMeta)
{
    auto chunkId = FromProto<TChunkId>(sampleRequest.chunk_id());

    // COMPAT(psushin)
    TKeyColumns chunkKeyColumns;
    auto optionalKeyColumnsExt = FindProtoExtension<TKeyColumnsExt>(chunkMeta.extensions());
    if (optionalKeyColumnsExt) {
        chunkKeyColumns = FromProto<TKeyColumns>(*optionalKeyColumnsExt);
    } else {
        auto schemaExt = GetProtoExtension<TTableSchemaExt>(chunkMeta.extensions());
        chunkKeyColumns = FromProto<TTableSchema>(schemaExt).GetKeyColumns();
    }

    bool isCompatibleKeyColumns =
        keyColumns.size() >= chunkKeyColumns.size() &&
        std::equal(
            chunkKeyColumns.begin(),
            chunkKeyColumns.end(),
            keyColumns.begin());

    // Requested key can be wider than stored.
    if (!isCompatibleKeyColumns) {
        auto error = TError("Incompatible key columns in chunk %v: requested key columns %v, chunk key columns %v",
            chunkId,
            keyColumns,
            chunkKeyColumns);
        ToProto(chunkSamples->mutable_error(), error);
        return error;
    }

    auto lowerKey = sampleRequest.has_lower_key()
        ? FromProto<TLegacyOwningKey>(sampleRequest.lower_key())
        : MinKey();

    auto upperKey = sampleRequest.has_upper_key()
        ? FromProto<TLegacyOwningKey>(sampleRequest.upper_key())
        : MaxKey();

    auto blockMeta = GetProtoExtension<TDataBlockMetaExt>(chunkMeta.extensions());

    std::vector<TLegacyOwningKey> samples;
    for (const auto& block : blockMeta.data_blocks()) {
        YT_VERIFY(block.has_last_key());
        auto key = FromProto<TLegacyOwningKey>(block.last_key());
        if (key >= lowerKey && key < upperKey) {
            samples.push_back(WidenKey(key, keyColumns.size()));
        }
    }

    // Don't return more than requested.
    std::random_shuffle(samples.begin(), samples.end());
    auto count = std::min(
        static_cast<int>(samples.size()),
        sampleRequest.sample_count());
    samples.erase(samples.begin() + count, samples.end());

    for (const auto& sample : samples) {
        auto* protoSample = chunkSamples->add_samples();
        protoSample->set_key_index(keySetWriter->WriteKey(sample));
        protoSample->set_incomplete(false);
        protoSample->set_weight(1);
    }

    return TError();
}

TError ProcessTableSamples(
    const TReqGetTableSamples::TSampleRequest& sampleRequest,
    TRspGetTableSamples::TChunkSamples* sampleResponse,
    ESamplingPolicy samplingPolicy,
    const TKeyColumns& keyColumns,
    i32 maxSampleSize,
    const TKeySetWriterPtr& keySetWriter,
    const TErrorOr<TRefCountedChunkMetaPtr>& metaOrError)
{
    auto chunkId = FromProto<TChunkId>(sampleRequest.chunk_id());
    try {
        THROW_ERROR_EXCEPTION_IF_FAILED(metaOrError, "Error getting meta of chunk %v",
            chunkId);
        const auto& meta = *metaOrError.Value();

        auto type = CheckedEnumCast<EChunkType>(meta.type());
        if (type != EChunkType::Table) {
            THROW_ERROR_EXCEPTION("Invalid type of chunk %v: expected %Qlv, actual %Qlv",
                chunkId,
                EChunkType::Table,
                type);
        }

        TError error;
        switch (samplingPolicy) {
            case ESamplingPolicy::Sorting:
                error = ProcessSortingSamples(sampleRequest, sampleResponse, keyColumns, maxSampleSize, keySetWriter, meta);
                break;

            case ESamplingPolicy::Partitioning:
                error = ProcessPartitioningSamples(sampleRequest, sampleResponse, keyColumns, keySetWriter, meta);
                break;

            default:
                YT_ABORT();
        }
        return error;
    } catch (const std::exception& ex) {
        auto error = TError(ex);
        ToProto(sampleResponse->mutable_error(), error);
        return error;
    }
}

std::vector<TError> ProcessGetTableSamplesRequest(
    const TReqGetTableSamples& request,
    TTypedServiceResponse<TRspGetTableSamples>& response,
    int requestCount,
    ESamplingPolicy samplingPolicy,
    const TKeyColumns& keyColumns,
    i32 maxSampleSize,
    const std::vector<TErrorOr<TRefCountedChunkMetaPtr>>& chunkMetas)
{
    std::vector<TError> errors;

    auto keySetWriter = New<TKeySetWriter>();
    for (int requestIndex = 0; requestIndex < requestCount; ++requestIndex) {
        const auto& sampleRequest = request.sample_requests(requestIndex);

        auto error = ProcessTableSamples(
            sampleRequest,
            response.add_sample_responses(),
            samplingPolicy,
            keyColumns,
            maxSampleSize,
            keySetWriter,
            chunkMetas[requestIndex]);
        if (!error.IsOK()) {
            auto chunkId = FromProto<TChunkId>(sampleRequest.chunk_id());
            error = error
                << TErrorAttribute("chunk_id", chunkId);
            errors.push_back(std::move(error));
        }
    }

    response.Attachments().push_back(keySetWriter->Finish());

    return errors;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
