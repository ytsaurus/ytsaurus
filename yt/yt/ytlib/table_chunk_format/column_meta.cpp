#include "column_meta.h"

#include "helpers.h"

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NTableChunkFormat {

using namespace NTransactionClient;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TTimestampSegmentMeta* protoTimestampSegmentMeta,
    const TTimestampSegmentMeta& timestampSegmentMeta)
{
    protoTimestampSegmentMeta->Clear();

    protoTimestampSegmentMeta->set_min_timestamp(timestampSegmentMeta.MinTimestamp);
    protoTimestampSegmentMeta->set_expected_writes_per_row(timestampSegmentMeta.ExpectedWritesPerRow);
    protoTimestampSegmentMeta->set_expected_deletes_per_row(timestampSegmentMeta.ExpectedDeletesPerRow);
}

void FromProto(
    TTimestampSegmentMeta* timestampSegmentMeta,
    const NProto::TTimestampSegmentMeta& protoTimestampSegmentMeta)
{
    timestampSegmentMeta->MinTimestamp = FromProto<TTimestamp>(protoTimestampSegmentMeta.min_timestamp());
    FromProto(&timestampSegmentMeta->ExpectedWritesPerRow, protoTimestampSegmentMeta.expected_writes_per_row());
    FromProto(&timestampSegmentMeta->ExpectedDeletesPerRow, protoTimestampSegmentMeta.expected_deletes_per_row());
}

void ToProto(
    NProto::TIntegerSegmentMeta* protoIntegerSegmentMeta,
    const TIntegerSegmentMeta& integerSegmentMeta)
{
    protoIntegerSegmentMeta->Clear();

    protoIntegerSegmentMeta->set_min_value(integerSegmentMeta.MinValue);
}

void FromProto(
    TIntegerSegmentMeta* integerSegmentMeta,
    const NProto::TIntegerSegmentMeta& protoIntegerSegmentMeta)
{
    FromProto(&integerSegmentMeta->MinValue, protoIntegerSegmentMeta.min_value());
}

void ToProto(
    NProto::TStringSegmentMeta* protoStringSegmentMeta,
    const TStringSegmentMeta& stringSegmentMeta)
{
    protoStringSegmentMeta->Clear();

    protoStringSegmentMeta->set_expected_length(stringSegmentMeta.ExpectedLength);
}

void FromProto(
    TStringSegmentMeta* stringSegmentMeta,
    const NProto::TStringSegmentMeta& protoStringSegmentMeta)
{
    FromProto(&stringSegmentMeta->ExpectedLength, protoStringSegmentMeta.expected_length());
}

void ToProto(
    NProto::TDenseVersionedSegmentMeta* protoDenseVersionedSegmentMeta,
    const TDenseVersionedSegmentMeta& denseVersionedSegmentMeta)
{
    protoDenseVersionedSegmentMeta->Clear();

    protoDenseVersionedSegmentMeta->set_expected_values_per_row(denseVersionedSegmentMeta.ExpectedValuesPerRow);
}

void FromProto(
    TDenseVersionedSegmentMeta* denseVersionedSegmentMeta,
    const NProto::TDenseVersionedSegmentMeta& protoDenseVersionedSegmentMeta)
{
    FromProto(&denseVersionedSegmentMeta->ExpectedValuesPerRow, protoDenseVersionedSegmentMeta.expected_values_per_row());
}

void ToProto(
    NProto::TSchemalessSegmentMeta* protoSchemalessSegmentMeta,
    const TSchemalessSegmentMeta& schemalessSegmentMeta)
{
    protoSchemalessSegmentMeta->Clear();

    protoSchemalessSegmentMeta->set_expected_bytes_per_row(schemalessSegmentMeta.ExpectedBytesPerRow);
}

void FromProto(
    TSchemalessSegmentMeta* schemalessSegmentMeta,
    const NProto::TSchemalessSegmentMeta& protoSchemalessSegmentMeta)
{
    FromProto(&schemalessSegmentMeta->ExpectedBytesPerRow, protoSchemalessSegmentMeta.expected_bytes_per_row());
}

////////////////////////////////////////////////////////////////////////////////

int TSegmentMeta::GetExtensionCount()
{
    int count = 0;
    #define XX(name, Name) \
        ++count;
    ITERATE_SEGMENT_META_EXTENSIONS(XX)
    #undef XX
    return count;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TSegmentMeta* protoSegmentMeta,
    const TSegmentMeta& segmentMeta)
{
    protoSegmentMeta->Clear();

    protoSegmentMeta->set_version(segmentMeta.Version);
    protoSegmentMeta->set_type(segmentMeta.Type);
    protoSegmentMeta->set_row_count(segmentMeta.RowCount);
    protoSegmentMeta->set_block_index(segmentMeta.BlockIndex);
    protoSegmentMeta->set_offset(segmentMeta.Offset);
    protoSegmentMeta->set_chunk_row_count(segmentMeta.ChunkRowCount);
    protoSegmentMeta->set_size(segmentMeta.Size);

    if (segmentMeta.IntegerSegmentMeta) {
        ToProto(
            protoSegmentMeta->MutableExtension(NProto::TIntegerSegmentMeta::integer_segment_meta),
            *segmentMeta.IntegerSegmentMeta);
    }

    if (segmentMeta.TimestampSegmentMeta) {
        ToProto(
            protoSegmentMeta->MutableExtension(NProto::TTimestampSegmentMeta::timestamp_segment_meta),
            *segmentMeta.TimestampSegmentMeta);
    }

    if (segmentMeta.StringSegmentMeta) {
        ToProto(
            protoSegmentMeta->MutableExtension(NProto::TStringSegmentMeta::string_segment_meta),
            *segmentMeta.StringSegmentMeta);
    }

    if (segmentMeta.DenseVersionedSegmentMeta) {
        ToProto(
            protoSegmentMeta->MutableExtension(NProto::TDenseVersionedSegmentMeta::dense_versioned_segment_meta),
            *segmentMeta.DenseVersionedSegmentMeta);
    }

    if (segmentMeta.SchemalessSegmentMeta) {
        ToProto(
            protoSegmentMeta->MutableExtension(NProto::TSchemalessSegmentMeta::schemaless_segment_meta),
            *segmentMeta.SchemalessSegmentMeta);
    }
}

void FromProto(
    TSegmentMeta* segmentMeta,
    const NProto::TSegmentMeta& protoSegmentMeta)
{
    FromProto(&segmentMeta->Version, protoSegmentMeta.version());
    FromProto(&segmentMeta->Type, protoSegmentMeta.type());
    FromProto(&segmentMeta->RowCount, protoSegmentMeta.row_count());
    FromProto(&segmentMeta->BlockIndex, protoSegmentMeta.block_index());
    FromProto(&segmentMeta->Offset, protoSegmentMeta.offset());
    FromProto(&segmentMeta->ChunkRowCount, protoSegmentMeta.chunk_row_count());
    FromProto(&segmentMeta->Size, protoSegmentMeta.size());

    if (protoSegmentMeta.HasExtension(NProto::TIntegerSegmentMeta::integer_segment_meta)) {
        const auto& protoIntegerSegmentMeta = protoSegmentMeta.GetExtension(NProto::TIntegerSegmentMeta::integer_segment_meta);
        TIntegerSegmentMeta integerSegmentMeta;
        FromProto(&integerSegmentMeta, protoIntegerSegmentMeta);
        segmentMeta->IntegerSegmentMeta = integerSegmentMeta;
    }

    if (protoSegmentMeta.HasExtension(NProto::TTimestampSegmentMeta::timestamp_segment_meta)) {
        const auto& protoTimestampSegmentMetaExt = protoSegmentMeta.GetExtension(NProto::TTimestampSegmentMeta::timestamp_segment_meta);
        TTimestampSegmentMeta timestampSegmentMeta;
        FromProto(&timestampSegmentMeta, protoTimestampSegmentMetaExt);
        segmentMeta->TimestampSegmentMeta = timestampSegmentMeta;
    }

    if (protoSegmentMeta.HasExtension(NProto::TStringSegmentMeta::string_segment_meta)) {
        const auto& protoStringSegmentMeta = protoSegmentMeta.GetExtension(NProto::TStringSegmentMeta::string_segment_meta);
        TStringSegmentMeta stringSegmentMeta;
        FromProto(&stringSegmentMeta, protoStringSegmentMeta);
        segmentMeta->StringSegmentMeta = stringSegmentMeta;
    }

    if (protoSegmentMeta.HasExtension(NProto::TDenseVersionedSegmentMeta::dense_versioned_segment_meta)) {
        const auto& protoDenseVersionedSegmentMeta = protoSegmentMeta.GetExtension(NProto::TDenseVersionedSegmentMeta::dense_versioned_segment_meta);
        TDenseVersionedSegmentMeta denseVersionedSegmentMeta;
        FromProto(&denseVersionedSegmentMeta, protoDenseVersionedSegmentMeta);
        segmentMeta->DenseVersionedSegmentMeta = denseVersionedSegmentMeta;
    }

    if (protoSegmentMeta.HasExtension(NProto::TSchemalessSegmentMeta::schemaless_segment_meta)) {
        const auto& protoSchemalessSegmentMeta = protoSegmentMeta.GetExtension(NProto::TSchemalessSegmentMeta::schemaless_segment_meta);
        TSchemalessSegmentMeta schemalessSegmentMeta;
        FromProto(&schemalessSegmentMeta, protoSchemalessSegmentMeta);
        segmentMeta->SchemalessSegmentMeta = schemalessSegmentMeta;
    }
}

void ToProto(
    NProto::TColumnMeta* protoColumnMeta,
    const TColumnMeta& columnMeta)
{
    protoColumnMeta->Clear();

    ToProto(protoColumnMeta->mutable_segments(), columnMeta.Segments);
}

void FromProto(
    TColumnMeta* columnMeta,
    const NProto::TColumnMeta& protoColumnMeta)
{
    FromProto(&columnMeta->Segments, protoColumnMeta.segments());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
