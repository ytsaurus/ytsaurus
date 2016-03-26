#include "string_column_reader.h"

#include "column_reader_detail.h"
#include "private.h"
#include "helpers.h"
#include "compressed_integer_vector.h"

#include <yt/core/misc/bitmap.h>
#include <yt/core/misc/zigzag.h>

namespace NYT {
namespace NTableChunkFormat {

using namespace NTableClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TStringValueExtractorBase
{
protected:
    TStringValueExtractorBase(const TSegmentMeta& segmentMeta)
        : StringMeta_(segmentMeta.GetExtension(TStringSegmentMeta::string_segment_meta))
    { }

    const NProto::TStringSegmentMeta& StringMeta_;

    TCompressedUnsignedVectorReader<ui32> OffsetsReader_;
    const char* StringData_;

    ui32 GetOffset(i64 offsetIndex) const
    {
        return StringMeta_.expected_length() * (offsetIndex + 1) + 
            ZigZagDecode32(OffsetsReader_[offsetIndex]);
    }

    void SetStringValue(TUnversionedValue* value, i64 offsetIndex) const
    {
        ui32 padding = offsetIndex == 0 ? 0 : GetOffset(offsetIndex - 1);
        value->Data.String = StringData_ + padding;
        value->Length = GetOffset(offsetIndex) - padding;

        value->Type = ValueType;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TDictionaryStringValueExtractorBase
    : public TStringValueExtractorBase<ValueType>
{
public:
    using TStringValueExtractorBase<ValueType>::TStringValueExtractorBase;

    void ExtractValue(TUnversionedValue* value, i64 valueIndex) const
    {
        if (IdsReader_[valueIndex] == 0) {
            value->Type = EValueType::Null;
        } else {
            SetStringValue(value, IdsReader_[valueIndex] - 1);
        }
    }

protected:
    TCompressedUnsignedVectorReader<ui32> IdsReader_;

    using TStringValueExtractorBase<ValueType>::SetStringValue;
    using TStringValueExtractorBase<ValueType>::OffsetsReader_;
    using TStringValueExtractorBase<ValueType>::StringData_;

    void InitDictionaryReader(const char* ptr)
    {
        IdsReader_ = TCompressedUnsignedVectorReader<ui32>(reinterpret_cast<const ui64*>(ptr));
        ptr += IdsReader_.GetByteSize();

        OffsetsReader_ = TCompressedUnsignedVectorReader<ui32>(reinterpret_cast<const ui64*>(ptr));
        ptr += OffsetsReader_.GetByteSize();

        StringData_ = ptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TDirectStringValueExtractorBase
    : public TStringValueExtractorBase<ValueType>
{
public:
    using TStringValueExtractorBase<ValueType>::TStringValueExtractorBase;

    void ExtractValue(TUnversionedValue* value, i64 valueIndex) const
    {
        if (NullBitmap_[valueIndex]) {
            value->Type = EValueType::Null;
        } else {
            SetStringValue(value, valueIndex);
        }
    }

protected:
    TReadOnlyBitmap<ui64> NullBitmap_;

    using TStringValueExtractorBase<ValueType>::SetStringValue;
    using TStringValueExtractorBase<ValueType>::OffsetsReader_;
    using TStringValueExtractorBase<ValueType>::StringData_;

    void InitDirectReader(const char* ptr)
    {
        OffsetsReader_ = TCompressedUnsignedVectorReader<ui32>(reinterpret_cast<const ui64*>(ptr));
        ptr += OffsetsReader_.GetByteSize();

        NullBitmap_ = TReadOnlyBitmap<ui64>(
            reinterpret_cast<const ui64*>(ptr),
            OffsetsReader_.GetSize());
        ptr += NullBitmap_.GetByteSize();

        StringData_ = ptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TDirectDenseVersionedStringValueExtractor
    : public TDenseVersionedValueExtractorBase
    , public TDirectStringValueExtractorBase<ValueType>
{
public:
    TDirectDenseVersionedStringValueExtractor(TRef data, const TSegmentMeta& meta, bool aggregate)
        : TDenseVersionedValueExtractorBase(meta, aggregate)
        , TDirectStringValueExtractorBase<ValueType>(meta)
    {
        const char* ptr = data.Begin();
        ptr += InitDenseReader(ptr);
        TDirectStringValueExtractorBase<ValueType>::InitDirectReader(ptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TDictionaryDenseVersionedStringValueExtractor
    : public TDenseVersionedValueExtractorBase
    , public TDictionaryStringValueExtractorBase<ValueType>
{
public:
    TDictionaryDenseVersionedStringValueExtractor(TRef data, const TSegmentMeta& meta, bool aggregate)
        : TDenseVersionedValueExtractorBase(meta, aggregate)
        , TDictionaryStringValueExtractorBase<ValueType>(meta)
    {
        const char* ptr = data.Begin();
        ptr += InitDenseReader(ptr);
        TDictionaryStringValueExtractorBase<ValueType>::InitDictionaryReader(ptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TDirectSparseVersionedStringValueExtractor
    : public TSparseVersionedValueExtractorBase
    , public TDirectStringValueExtractorBase<ValueType>
{
public:
    TDirectSparseVersionedStringValueExtractor(TRef data, const TSegmentMeta& meta, bool aggregate)
        : TSparseVersionedValueExtractorBase(meta, aggregate)
        , TDirectStringValueExtractorBase<ValueType>(meta)
    {
        const char* ptr = data.Begin();
        ptr += InitSparseReader(ptr);
        TDirectStringValueExtractorBase<ValueType>::InitDirectReader(ptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TDictionarySparseVersionedStringValueExtractor
    : public TSparseVersionedValueExtractorBase
    , public TDictionaryStringValueExtractorBase<ValueType>
{
public:
    TDictionarySparseVersionedStringValueExtractor(TRef data, const TSegmentMeta& meta, bool aggregate)
        : TSparseVersionedValueExtractorBase(meta, aggregate)
        , TDictionaryStringValueExtractorBase<ValueType>(meta)
    {
        const char* ptr = data.Begin();
        ptr += InitSparseReader(ptr);
        TDictionaryStringValueExtractorBase<ValueType>::InitDictionaryReader(ptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDirectRLEStringUnversionedValueExtractor
    : public TRLEValueExtractorBase
    , public TDirectStringValueExtractorBase<EValueType::String>
{
public:
    TDirectRLEStringUnversionedValueExtractor(TRef data, const TSegmentMeta& meta)
        : TDirectStringValueExtractorBase(meta)
    {
        const char* ptr = data.Begin();
        RowIndexReader_ = TCompressedUnsignedVectorReader<ui64>(reinterpret_cast<const ui64*>(ptr));
        ptr += RowIndexReader_.GetByteSize();

        InitDirectReader(ptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDictionaryRLEStringUnversionedValueExtractor
    : public TRLEValueExtractorBase
    , public TDictionaryStringValueExtractorBase<EValueType::String>
{
public:
    TDictionaryRLEStringUnversionedValueExtractor(TRef data, const TSegmentMeta& meta)
        : TDictionaryStringValueExtractorBase(meta)
    {
        const char* ptr = data.Begin();
        RowIndexReader_ = TCompressedUnsignedVectorReader<ui64>(reinterpret_cast<const ui64*>(ptr));
        ptr += RowIndexReader_.GetByteSize();

        InitDictionaryReader(ptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDictionaryDenseStringUnversionedValueExtractor
    : public TDictionaryStringValueExtractorBase<EValueType::String>
{
public:
    TDictionaryDenseStringUnversionedValueExtractor(TRef data, const TSegmentMeta& meta)
        : TDictionaryStringValueExtractorBase(meta)
    {
        const char* ptr = data.Begin();
        InitDictionaryReader(ptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDirectDenseStringUnversionedValueExtractor
    : public TDirectStringValueExtractorBase<EValueType::String>
{
public:
    TDirectDenseStringUnversionedValueExtractor(TRef data, const TSegmentMeta& meta)
        : TDirectStringValueExtractorBase(meta)
    {
        InitDirectReader(data.Begin());
        YCHECK(meta.row_count() == OffsetsReader_.GetSize());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TVersionedStringColumnReader
    : public TVersionedColumnReaderBase
{
public:
    TVersionedStringColumnReader(const TColumnMeta& columnMeta, int columnId, bool aggregate)
        : TVersionedColumnReaderBase(columnMeta, columnId, aggregate)
    { }

private:
    virtual std::unique_ptr<IVersionedSegmentReader> CreateSegmentReader(int segmentIndex) override
    {
        using TDirectDenseReader = TDenseVersionedSegmentReader<
            TDirectDenseVersionedStringValueExtractor<ValueType>>;
        using TDictionaryDenseReader = TDenseVersionedSegmentReader<
            TDictionaryDenseVersionedStringValueExtractor<ValueType>>;
        using TDirectSparseReader = TSparseVersionedSegmentReader<
            TDirectSparseVersionedStringValueExtractor<ValueType>>;
        using TDictionarySparseReader = TSparseVersionedSegmentReader<
            TDictionarySparseVersionedStringValueExtractor<ValueType>>;

        const auto& meta = ColumnMeta_.segments(segmentIndex);
        auto segmentType = EVersionedStringSegmentType(meta.type());

        switch (segmentType) {
            case EVersionedStringSegmentType::DirectDense:
                return DoCreateSegmentReader<TDirectDenseReader>(meta);

            case EVersionedStringSegmentType::DictionaryDense:
                return DoCreateSegmentReader<TDictionaryDenseReader>(meta);

            case EVersionedStringSegmentType::DirectSparse:
                return DoCreateSegmentReader<TDirectSparseReader>(meta);

            case EVersionedStringSegmentType::DictionarySparse:
                return DoCreateSegmentReader<TDictionarySparseReader>(meta);

            default:
                YUNREACHABLE();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedColumnReader> CreateVersionedStringColumnReader(
    const TColumnMeta& columnMeta,
    int columnId,
    bool aggregate)
{
    return std::make_unique<TVersionedStringColumnReader<EValueType::String>>(
        columnMeta,
        columnId,
        aggregate);
}

std::unique_ptr<IVersionedColumnReader> CreateVersionedAnyColumnReader(
    const TColumnMeta& columnMeta,
    int columnId,
    bool aggregate)
{
    return std::make_unique<TVersionedStringColumnReader<EValueType::Any>>(
        columnMeta,
        columnId,
        aggregate);
}

////////////////////////////////////////////////////////////////////////////////

class TUnversionedStringColumnReader
    : public TUnversionedColumnReaderBase
{
public:
    TUnversionedStringColumnReader(const TColumnMeta& columnMeta, int columnIndex, int columnId)
        : TUnversionedColumnReaderBase(
            columnMeta,
            columnIndex,
            columnId)
    { }

    virtual std::pair<i64, i64> GetEqualRange(
        const TUnversionedValue& value,
        i64 lowerRowIndex,
        i64 upperRowIndex) override
    {
        return DoGetEqualRange<EValueType::String>(
            value, 
            lowerRowIndex, 
            upperRowIndex);
    }

private:
    virtual std::unique_ptr<IUnversionedSegmentReader> CreateSegmentReader(int segmentIndex) override
    {
        typedef TDenseUnversionedSegmentReader<
            EValueType::String,
            TDirectDenseStringUnversionedValueExtractor> TDirectDenseReader;

        typedef TDenseUnversionedSegmentReader<
            EValueType::String,
            TDictionaryDenseStringUnversionedValueExtractor> TDictionaryDenseReader;

        typedef TRLEUnversionedSegmentReader<
            EValueType::String,
            TDirectRLEStringUnversionedValueExtractor> TDirectRLEReader;

        typedef TRLEUnversionedSegmentReader<
            EValueType::String,
            TDictionaryRLEStringUnversionedValueExtractor> TDictionaryRLEReader;

        const auto& meta = ColumnMeta_.segments(segmentIndex);
        auto segmentType = EUnversionedStringSegmentType(meta.type());

        switch (segmentType) {
            case EUnversionedStringSegmentType::DirectDense:
                return DoCreateSegmentReader<TDirectDenseReader>(meta);

            case EUnversionedStringSegmentType::DictionaryDense:
                return DoCreateSegmentReader<TDictionaryDenseReader>(meta);

            case EUnversionedStringSegmentType::DirectRLE:
                return DoCreateSegmentReader<TDirectRLEReader>(meta);

            case EUnversionedStringSegmentType::DictionaryRLE:
                return DoCreateSegmentReader<TDictionaryRLEReader>(meta);

            default:
                YUNREACHABLE();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedStringColumnReader(
    const TColumnMeta& columnMeta,
    int columnIndex,
    int columnId)
{
    return std::make_unique<TUnversionedStringColumnReader>(
        columnMeta,
        columnIndex,
        columnId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormat
} // namespace NYT
