#pragma once

#include "column_writer.h"
#include "private.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/ytlib/new_table_client/prepared_meta.h>

#include <yt/yt/core/misc/bitmap.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

class TColumnWriterBase
    : public IValueColumnWriter
{
public:
    explicit TColumnWriterBase(TDataBlockWriter* blockWriter);

    TSharedRef FinishBlock(int blockIndex) override;

    const NProto::TColumnMeta& ColumnMeta() const override;
    i64 GetMetaSize() const override;

protected:
    TDataBlockWriter* const BlockWriter_;

    i64 RowCount_ = 0;

    i64 MetaSize_ = 0;
    NProto::TColumnMeta ColumnMeta_;
    std::vector<NProto::TSegmentMeta> CurrentBlockSegments_;
    // Segment metas are stored in block in new columnar format.
    std::vector<TSharedRef> CurrentBlockSegmentMetas_;

    void DumpSegment(TSegmentInfo* segmentInfo, TSharedRef inBlockMeta);

    i64 GetOffset() const;
};

////////////////////////////////////////////////////////////////////////////////

class TVersionedColumnWriterBase
    : public TColumnWriterBase
{
public:
    TVersionedColumnWriterBase(
        int columnId,
        const NTableClient::TColumnSchema& columnSchema,
        TDataBlockWriter* blockWriter);

    i32 GetCurrentSegmentSize() const override;

    void WriteUnversionedValues(TRange<NTableClient::TUnversionedRow> rows) override;

protected:
    const int ColumnId_;
    const bool Aggregate_;
    const bool Hunk_;

    i64 EmptyPendingRowCount_ = 0;

    std::vector<ui32> TimestampIndexes_;
    TBitmapOutput NullBitmap_;
    TBitmapOutput AggregateBitmap_;

    std::vector<ui32> ValuesPerRow_;

    TTimestampIndex MaxTimestampIndex_;


    void Reset();

    void AddValues(
        TRange<NTableClient::TVersionedRow> rows,
        std::function<bool (const NTableClient::TVersionedValue& value)> onValue);

    void DumpVersionedData(TSegmentInfo* segmentInfo, NNewTableClient::TMultiValueIndexMeta* rawIndexMeta);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
