#include "floating_point_column_reader.h"

#include "column_reader_detail.h"
#include "helpers.h"

#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/logical_type.h>

#include <yt/yt/core/misc/bitmap.h>

namespace NYT::NTableChunkFormat {

using namespace NTableClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TFloatingPointValueExtractorBase
{
public:
    void ExtractValue(TUnversionedValue* value, i64 valueIndex, int id, EValueFlags flags) const
    {
        YT_ASSERT(None(flags & EValueFlags::Hunk));
        if (NullBitmap_[valueIndex]) {
            *value = MakeUnversionedSentinelValue(EValueType::Null, id, flags);
        } else {
            *value = MakeUnversionedDoubleValue(Values_[valueIndex], id, flags);
        }
    }

protected:
    TRange<T> Values_;
    TReadOnlyBitmap NullBitmap_;

    const char* InitValueReader(const char* ptr)
    {
        ui64 valueCount = *reinterpret_cast<const ui64*>(ptr);
        ptr += sizeof(ui64);

        Values_ = TRange(reinterpret_cast<const T*>(ptr), valueCount);
        ptr += sizeof(T) * valueCount;

        NullBitmap_ = TReadOnlyBitmap(Values_.end(), valueCount);
        ptr += AlignUp(NullBitmap_.GetByteSize(), SerializationAlignment);

        return ptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDirectDenseVersionedDoubleValueExtractor
    : public TDenseVersionedValueExtractorBase
    , public TFloatingPointValueExtractorBase<double>
{
public:
    TDirectDenseVersionedDoubleValueExtractor(
        TRef data,
        const NProto::TSegmentMeta& meta,
        bool aggregate)
        : TDenseVersionedValueExtractorBase(meta, aggregate)
    {
        const char* ptr = data.Begin();
        ptr = InitDenseReader(ptr);
        ptr = this->InitValueReader(ptr);
        YT_VERIFY(ptr == data.End());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDirectSparseVersionedDoubleValueExtractor
    : public TSparseVersionedValueExtractorBase
    , public TFloatingPointValueExtractorBase<double>
{
public:
    TDirectSparseVersionedDoubleValueExtractor(
        TRef data,
        const NProto::TSegmentMeta& /*meta*/,
        bool aggregate)
        : TSparseVersionedValueExtractorBase(aggregate)
    {
        const char* ptr = data.Begin();
        ptr = InitSparseReader(ptr);
        ptr = this->InitValueReader(ptr);
        YT_VERIFY(ptr == data.End());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVersionedDoubleColumnReader
    : public TVersionedColumnReaderBase
{
public:
    using TVersionedColumnReaderBase::TVersionedColumnReaderBase;

private:
    std::unique_ptr<IVersionedSegmentReader> CreateSegmentReader(int segmentIndex) override
    {
        using TDirectDenseReader = TDenseVersionedSegmentReader<TDirectDenseVersionedDoubleValueExtractor>;
        using TDirectSparseReader = TSparseVersionedSegmentReader<TDirectSparseVersionedDoubleValueExtractor>;

        const auto& meta = ColumnMeta_.segments(segmentIndex);
        auto dense = meta.HasExtension(NProto::TDenseVersionedSegmentMeta::dense_versioned_segment_meta);

        if (dense) {
            return DoCreateSegmentReader<TDirectDenseReader>(meta);
        } else {
            return DoCreateSegmentReader<TDirectSparseReader>(meta);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedColumnReader> CreateVersionedDoubleColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnId,
    const TColumnSchema& columnSchema)
{
    return std::make_unique<TVersionedDoubleColumnReader>(
        columnMeta,
        columnId,
        columnSchema);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TUnversionedFloatingPointValueExtractor
    : public TFloatingPointValueExtractorBase<T>
{
public:
    TUnversionedFloatingPointValueExtractor(
        TRef data,
        const NProto::TSegmentMeta& /*meta*/)
    {
        const char* ptr = data.Begin();
        ptr = this->InitValueReader(data.Begin());
        YT_VERIFY(ptr == data.End());
    }

    int GetBatchColumnCount()
    {
        return 1;
    }

    void ReadColumnarBatch(
        i64 startRowIndex,
        i64 rowCount,
        TMutableRange<IUnversionedColumnarRowBatch::TColumn> columns)
    {
        YT_VERIFY(columns.size() == 1);
        auto& column = columns[0];
        ReadColumnarFloatingPointValues(
            &column,
            startRowIndex,
            rowCount,
            this->Values_);
        ReadColumnarNullBitmap(
            &column,
            startRowIndex,
            rowCount,
            this->NullBitmap_.GetData());
    }

    i64 EstimateDataWeight(i64 lowerRowIndex, i64 upperRowIndex)
    {
        return (upperRowIndex - lowerRowIndex) * sizeof(T);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TUnversionedFloatingPointColumnReader
    : public TUnversionedColumnReaderBase
{
public:
    static_assert(std::is_floating_point_v<T>);

    using TUnversionedColumnReaderBase::TUnversionedColumnReaderBase;

    std::pair<i64, i64> GetEqualRange(
        const TUnversionedValue& value,
        i64 lowerRowIndex,
        i64 upperRowIndex) override
    {
        return DoGetEqualRange<EValueType::Double>(
            value,
            lowerRowIndex,
            upperRowIndex);
    }

private:
    std::unique_ptr<IUnversionedSegmentReader> CreateSegmentReader(int segmentIndex, bool /*scan*/) override
    {
        using TSegmentReader = TDenseUnversionedSegmentReader<
            EValueType::Double,
            TUnversionedFloatingPointValueExtractor<T>>;

        const auto& meta = ColumnMeta_.segments(segmentIndex);
        return DoCreateSegmentReader<TSegmentReader>(meta);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
std::unique_ptr<IUnversionedColumnReader> CreateUnversionedFloatingPointColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnIndex,
    int columnId,
    std::optional<ESortOrder> sortOrder,
    const TColumnSchema& columnSchema)
{
    return std::make_unique<TUnversionedFloatingPointColumnReader<T>>(
        columnMeta,
        columnIndex,
        columnId,
        sortOrder,
        columnSchema);
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedColumnReader> CreateVersionedDoubleColumnReader(
    const NProto::TColumnMeta& columnMeta,
    int columnId,
    const TColumnSchema& columnSchema);

template
std::unique_ptr<IUnversionedColumnReader> CreateUnversionedFloatingPointColumnReader<float>(
    const NProto::TColumnMeta& columnMeta,
    int columnIndex,
    int columnId,
    std::optional<ESortOrder> sortOrder,
    const TColumnSchema& columnSchema);

template
std::unique_ptr<IUnversionedColumnReader> CreateUnversionedFloatingPointColumnReader<double>(
    const NProto::TColumnMeta& columnMeta,
    int columnIndex,
    int columnId,
    std::optional<ESortOrder> sortOrder,
    const TColumnSchema& columnSchema);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
