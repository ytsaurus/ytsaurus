#pragma once

#include "public.h"

#include <yt/yt_proto/yt/client/table_chunk_format/proto/column_meta.pb.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

struct IColumnWriterBase
    : public TNonCopyable
{
    virtual ~IColumnWriterBase() = default;

    virtual void FinishBlock(int blockIndex) = 0;

    //! For testing purposes.
    virtual void FinishCurrentSegment() = 0;

    virtual i32 GetCurrentSegmentSize() const = 0;
    virtual const NProto::TColumnMeta& ColumnMeta() const = 0;
    virtual i64 GetMetaSize() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IValueColumnWriter
    : public IColumnWriterBase
{
    //! Batch interface for writing values from several rows at once.
    virtual void WriteVersionedValues(TRange<NTableClient::TVersionedRow> rows) = 0;

    //! Batch interface for writing values from several rows at once.
    virtual void WriteUnversionedValues(TRange<NTableClient::TUnversionedRow> rows) = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateUnversionedColumnWriter(
    int columnIndex,
    const NTableClient::TColumnSchema& columnSchema,
    TDataBlockWriter* blockWriter,
    int maxValueCount = DefaultMaxSegmentValueCount);

std::unique_ptr<IValueColumnWriter> CreateVersionedColumnWriter(
    int columnId,
    const NTableClient::TColumnSchema& columnSchema,
    TDataBlockWriter* blockWriter,
    int maxValueCount = DefaultMaxSegmentValueCount);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
