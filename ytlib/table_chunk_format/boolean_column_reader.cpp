#include "boolean_column_reader.h"

#include "column_reader_detail.h"
#include "helpers.h"

#include <yt/core/misc/bitmap.h>

namespace NYT::NTableChunkFormat {

using namespace NTableClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

class TBooleanValueExtractorBase
{
public:
    void ExtractValue(TUnversionedValue* value, i64 valueIndex, int id, bool aggregate) const
    {
        if (NullBitmap_[valueIndex]) {
            *value = MakeUnversionedSentinelValue(EValueType::Null, id, aggregate);
        } else {
            *value = MakeUnversionedBooleanValue(Values_[valueIndex], id, aggregate);
        }
    }

protected:
    TReadOnlyBitmap<ui64> Values_;
    TReadOnlyBitmap<ui64> NullBitmap_;

    size_t InitValueReader(const char* ptr)
    {
        const char* begin = ptr;

        ui64 valueCount = *reinterpret_cast<const ui64*>(ptr);
        ptr += sizeof(ui64);

        Values_ = TReadOnlyBitmap<ui64>(
            reinterpret_cast<const ui64*>(ptr),
            valueCount);
        ptr += Values_.GetByteSize();

        NullBitmap_ = TReadOnlyBitmap<ui64>(
            reinterpret_cast<const ui64*>(ptr),
            valueCount);
        ptr += NullBitmap_.GetByteSize();

        return ptr - begin;
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
        ptr += InitDenseReader(ptr);
        ptr += InitValueReader(ptr);
        YCHECK(ptr == data.End());
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
        const TSegmentMeta& meta, 
        bool aggregate)
        : TSparseVersionedValueExtractorBase(meta, aggregate)
    {
        const char* ptr = data.Begin();
        ptr += InitSparseReader(ptr);
        ptr += InitValueReader(ptr);
        YCHECK(ptr == data.End());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVersionedBooleanColumnReader
    : public TVersionedColumnReaderBase
{
public:
    TVersionedBooleanColumnReader(const TColumnMeta& columnMeta, int columnId, bool aggregate)
        : TVersionedColumnReaderBase(columnMeta, columnId, aggregate)
    { }

private:
    virtual std::unique_ptr<IVersionedSegmentReader> CreateSegmentReader(int segmentIndex) override
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
    bool aggregate)
{
    return std::make_unique<TVersionedBooleanColumnReader>(
        columnMeta,
        columnId,
        aggregate);
}

////////////////////////////////////////////////////////////////////////////////

class TUnversionedBooleanValueExtractor
    : public TBooleanValueExtractorBase
{
public:
    TUnversionedBooleanValueExtractor(TRef data, const TSegmentMeta& meta)
    {
        const char* ptr = data.Begin();
        ptr += InitValueReader(data.Begin());
        YCHECK(ptr == data.End());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TUnversionedBooleanColumnReader
    : public TUnversionedColumnReaderBase
{
public:
    TUnversionedBooleanColumnReader(const TColumnMeta& columnMeta, int columnIndex, int columnId)
        : TUnversionedColumnReaderBase(columnMeta, columnIndex, columnId)
    { }

    virtual std::pair<i64, i64> GetEqualRange(
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
    virtual std::unique_ptr<IUnversionedSegmentReader> CreateSegmentReader(int segmentIndex, bool /* scan */) override
    {
        using TSegmentReader = TDenseUnversionedSegmentReader<
            EValueType::Boolean,
            TUnversionedBooleanValueExtractor> ;

        const auto& meta = ColumnMeta_.segments(segmentIndex);
        return DoCreateSegmentReader<TSegmentReader>(meta);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedBooleanColumnReader(
    const TColumnMeta& columnMeta,
    int columnIndex,
    int columnId)
{
    return std::make_unique<TUnversionedBooleanColumnReader>(
        columnMeta,
        columnIndex,
        columnId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
