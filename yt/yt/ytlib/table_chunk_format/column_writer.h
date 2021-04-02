#pragma once

#include "public.h"

#include <yt/yt_proto/yt/client/table_chunk_format/proto/column_meta.pb.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/core/misc/ref.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

struct IColumnWriterBase
    : public TNonCopyable
{
    virtual ~IColumnWriterBase() = default;

    virtual void FinishBlock(int blockIndex) = 0;

    // Useful for test purposes.
    virtual void FinishCurrentSegment() = 0;

    virtual i32 GetCurrentSegmentSize() const = 0;

    virtual const NProto::TColumnMeta& ColumnMeta() const = 0;

    virtual i64 GetMetaSize() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IValueColumnWriter
    : public IColumnWriterBase
{
    // Batch interface for writing values from several rows at once.
    virtual void WriteValues(TRange<NTableClient::TVersionedRow> rows) = 0;

    // Batch interface for writing values from several rows at once.
    virtual void WriteUnversionedValues(TRange<NTableClient::TUnversionedRow> rows) = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IValueColumnWriter> CreateUnversionedColumnWriter(
    const NTableClient::TColumnSchema& columnSchema,
    int columnIndex,
    TDataBlockWriter* blockWriter);

std::unique_ptr<IValueColumnWriter> CreateVersionedColumnWriter(
    const NTableClient::TColumnSchema& columnSchema,
    int columnId,
    TDataBlockWriter* blockWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
