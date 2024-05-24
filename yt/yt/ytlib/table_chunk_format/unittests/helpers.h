#pragma once

#include <yt/yt/ytlib/table_chunk_format/helpers.h>

#include <yt_proto/yt/client/table_chunk_format/proto/column_meta.pb.h>

namespace NYT::NTableChunkFormat {
namespace {

////////////////////////////////////////////////////////////////////////////////

inline NProto::TSegmentMeta CreateSimpleSegmentMeta()
{
    NProto::TSegmentMeta segmentMeta;
    segmentMeta.set_version(1);
    segmentMeta.set_type(2);
    segmentMeta.set_row_count(3);
    segmentMeta.set_block_index(4);
    segmentMeta.set_offset(5);
    segmentMeta.set_chunk_row_count(6);
    segmentMeta.set_size(7);

    {
        auto* meta = segmentMeta.MutableExtension(NProto::TTimestampSegmentMeta::timestamp_segment_meta);
        meta->set_min_timestamp(0);
        meta->set_expected_writes_per_row(0);
        meta->set_expected_deletes_per_row(0);
    }

    {
        auto* meta = segmentMeta.MutableExtension(NProto::TIntegerSegmentMeta::integer_segment_meta);
        meta->set_min_value(0);
    }

    {
        auto* meta = segmentMeta.MutableExtension(NProto::TStringSegmentMeta::string_segment_meta);
        meta->set_expected_length(0);
    }

    {
        auto* meta = segmentMeta.MutableExtension(NProto::TDenseVersionedSegmentMeta::dense_versioned_segment_meta);
        meta->set_expected_values_per_row(0);
    }

    {
        auto* meta = segmentMeta.MutableExtension(NProto::TSchemalessSegmentMeta::schemaless_segment_meta);
        meta->set_expected_bytes_per_row(0);
    }
    return segmentMeta;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableChunkFormat
