#include "boolean_column_reader.h"

#include "column_reader_detail.h"
#include "helpers.h"

#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/logical_type.h>

#include <yt/yt/core/misc/bitmap.h>

namespace NYT::NTableChunkFormat {

using namespace NTableClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

class TBooleanValueExtractorBase
{
public:
    void ExtractValue(TUnversionedValue* value, i64 valueIndex, int id, EValueFlags flags) const
    {
        YT_ASSERT(None(flags & EValueFlags::Hunk));
        if (NullBitmap_[valueIndex]) {
            *value = MakeUnversionedSentinelValue(EValueType::Null, id, flags);
        } else {
            *value = MakeUnversionedBooleanValue(Values_[valueIndex], id, flags);
        }
    }

protected:
    TReadOnlyBitmap Values_;
    TReadOnlyBitmap NullBitmap_;

    const char* InitValueReader(const char* ptr)
    {
        ui64 valueCount = *reinterpret_cast<const ui64*>(ptr);
        ptr += sizeof(ui64);

        Values_ = TReadOnlyBitmap(
            reinterpret_cast<const ui64*>(ptr),
            valueCount);
        ptr += AlignUp(Values_.GetByteSize(), SerializationAlignment);

        NullBitmap_ = TReadOnlyBitmap(
            reinterpret_cast<const ui64*>(ptr),
            valueCount);
        ptr += AlignUp(NullBitmap_.GetByteSize(), SerializationAlignment);

        return ptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDenseVersionedBooleanValueExtractor
    : public TDenseVersionedValueExtractorBase
    , public TBooleanValueExtractorBase
{
public:
    TDenseVersionedBooleanValueExtractor(
        TRef data,
        const TSegmentMeta& meta,
        bool aggregate)
        : TDenseVersionedValueExtractorBase(meta, aggregate)
    {
        const char* ptr = data.Begin();
        ptr = InitDenseReader(ptr);
        ptr = InitValueReader(ptr);
        YT_VERIFY(ptr == data.End());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSparseVersionedBooleanValueExtractor
    : public TSparseVersionedValueExtractorBase
    , public TBooleanValueExtractorBase
{
public:
    TSparseVersionedBooleanValueExtractor(
        TRef data,
        const TSegmentMeta& /*meta*/,
        bool aggregate)
        : TSparseVersionedValueExtractorBase(aggregate)
    {
        const char* ptr = data.Begin();
        ptr = InitSparseReader(ptr);
        ptr = InitValueReader(ptr);
        YT_VERIFY(ptr == data.End());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVersionedBooleanColumnReader
    : public TVersionedColumnReaderBase
{
public:
    using TVersionedColumnReaderBase::TVersionedColumnReaderBase;

private:
    std::unique_ptr<IVersionedSegmentReader> CreateSegmentReader(int segmentIndex) override
    {
        using TDirectDenseReader = TDenseVersionedSegmentReader<TDenseVersionedBooleanValueExtractor>;
        using TDirectSparseReader = TSparseVersionedSegmentReader<TSparseVersionedBooleanValueExtractor>;

        const auto& meta = ColumnMeta_.segments(segmentIndex);
        auto dense = meta.HasExtension(TDenseVersionedSegmentMeta::dense_versioned_segment_meta);

        if (dense) {
            return DoCreateSegmentReader<TDirectDenseReader>(meta);
        } else {
            return DoCreateSegmentReader<TDirectSparseReader>(meta);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedColumnReader> CreateVersionedBooleanColumnReader(
    const TColumnMeta& columnMeta,
    int columnId,
    const TColumnSchema& columnSchema)
{
    return std::make_unique<TVersionedBooleanColumnReader>(
        columnMeta,
        columnId,
        columnSchema);
}

////////////////////////////////////////////////////////////////////////////////

class TUnversionedBooleanValueExtractor
    : public TBooleanValueExtractorBase
{
public:
    TUnversionedBooleanValueExtractor(
        TRef data,
        const TSegmentMeta& /*meta*/)
    {
        const char* ptr = data.Begin();
        ptr = InitValueReader(ptr);
        YT_VERIFY(ptr == data.End());
    }

    int GetBatchColumnCount()
    {
        return 1;
    }

    void ReadColumnarBatch(
        i64 startRowIndex,
        i64 rowCount,
        TMutableRange<NTableClient::IUnversionedColumnarRowBatch::TColumn> columns)
    {
        YT_VERIFY(columns.size() == 1);
        auto& column = columns[0];
        ReadColumnarBooleanValues(
            &column,
            startRowIndex,
            rowCount,
            Values_.GetData());
        ReadColumnarNullBitmap(
            &column,
            startRowIndex,
            rowCount,
            NullBitmap_.GetData());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TUnversionedBooleanColumnReader
    : public TUnversionedColumnReaderBase
{
public:
    using TUnversionedColumnReaderBase::TUnversionedColumnReaderBase;

    std::pair<i64, i64> GetEqualRange(
        const TUnversionedValue& value,
        i64 lowerRowIndex,
        i64 upperRowIndex) override
    {
        return DoGetEqualRange<EValueType::Boolean>(
            value,
            lowerRowIndex,
            upperRowIndex);
    }

private:
    std::unique_ptr<IUnversionedSegmentReader> CreateSegmentReader(int segmentIndex, bool /*scan*/) override
    {
        using TSegmentReader = TDenseUnversionedSegmentReader<
            EValueType::Boolean,
            TUnversionedBooleanValueExtractor>;

        const auto& meta = ColumnMeta_.segments(segmentIndex);
        return DoCreateSegmentReader<TSegmentReader>(meta);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedBooleanColumnReader(
    const TColumnMeta& columnMeta,
    int columnIndex,
    int columnId,
    std::optional<ESortOrder> sortOrder,
    const NTableClient::TColumnSchema& columnSchema)
{
    return std::make_unique<TUnversionedBooleanColumnReader>(
        columnMeta,
        columnIndex,
        columnId,
        sortOrder,
        columnSchema);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
