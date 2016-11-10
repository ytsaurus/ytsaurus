#include "string_column_reader.h"

#include "column_reader_detail.h"
#include "private.h"
#include "helpers.h"
#include "compressed_integer_vector.h"

#include <yt/ytlib/table_client/helpers.h>

#include <yt/core/yson/lexer.h>

#include <yt/core/misc/bitmap.h>
#include <yt/core/misc/zigzag.h>

namespace NYT {
namespace NTableChunkFormat {

using namespace NTableClient;
using namespace NProto;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan, bool UnpackValue>
class TStringValueExtractorBase
{
protected:
    TStringValueExtractorBase(const TSegmentMeta& segmentMeta)
        : StringMeta_(segmentMeta.GetExtension(TStringSegmentMeta::string_segment_meta))
    { }

    const NProto::TStringSegmentMeta& StringMeta_;
    mutable TStatelessLexer Lexer_;

    using TOffsetsReader = TCompressedUnsignedVectorReader<ui32, Scan>;

    TOffsetsReader OffsetsReader_;
    const char* StringData_;

    ui32 GetOffset(i64 offsetIndex) const
    {
        return StringMeta_.expected_length() * (offsetIndex + 1) + 
            ZigZagDecode32(OffsetsReader_[offsetIndex]);
    }

    void SetStringValue(TUnversionedValue* value, i64 offsetIndex, int id, bool aggregate) const
    {
        ui32 padding = offsetIndex == 0 ? 0 : GetOffset(offsetIndex - 1);
        const char* begin = StringData_ + padding;
        ui32 length = GetOffset(offsetIndex) - padding;
        auto string = TStringBuf(begin, length);

        switch (ValueType) {
            case EValueType::String:
                *value = MakeUnversionedStringValue(string, id, aggregate);
                break;

            case EValueType::Any:
                if (UnpackValue) {
                    YCHECK(aggregate == false);
                    *value = MakeUnversionedValue(string, id, Lexer_);
                } else {
                    *value = MakeUnversionedAnyValue(string, id, aggregate);
                }
                break;

            default:
                Y_UNREACHABLE();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan, bool UnpackValue>
class TDictionaryStringValueExtractorBase
    : public TStringValueExtractorBase<ValueType, Scan, UnpackValue>
{
public:
    using TStringValueExtractorBase<ValueType, Scan, UnpackValue>::TStringValueExtractorBase;

    void ExtractValue(TUnversionedValue* value, i64 valueIndex, int id, bool aggregate) const
    {
        auto dictionaryId = IdsReader_[valueIndex];
        if (dictionaryId == 0) {
            *value = MakeUnversionedSentinelValue(EValueType::Null, id, aggregate);
        } else {
            SetStringValue(value, dictionaryId - 1, id, aggregate);
        }
    }

protected:
    using TBase = TStringValueExtractorBase<ValueType, Scan, UnpackValue>;
    using TIdsReader = TCompressedUnsignedVectorReader<ui32, Scan>;
    TIdsReader IdsReader_;

    using TBase::SetStringValue;
    using TBase::OffsetsReader_;
    using TBase::StringData_;
    using typename TBase::TOffsetsReader;

    void InitDictionaryReader(const char* ptr)
    {
        IdsReader_ = TIdsReader(reinterpret_cast<const ui64*>(ptr));
        ptr += IdsReader_.GetByteSize();

        OffsetsReader_ = TOffsetsReader(reinterpret_cast<const ui64*>(ptr));
        ptr += OffsetsReader_.GetByteSize();

        StringData_ = ptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan, bool UnpackValue>
class TDirectStringValueExtractorBase
    : public TStringValueExtractorBase<ValueType, Scan, UnpackValue>
{
public:
    using TStringValueExtractorBase<ValueType, Scan, UnpackValue>::TStringValueExtractorBase;

    void ExtractValue(TUnversionedValue* value, i64 valueIndex, int id, bool aggregate) const
    {
        if (NullBitmap_[valueIndex]) {
            *value = MakeUnversionedSentinelValue(EValueType::Null, id, aggregate);
        } else {
            SetStringValue(value, valueIndex, id, aggregate);
        }
    }

protected:
    TReadOnlyBitmap<ui64> NullBitmap_;

    using TBase = TStringValueExtractorBase<ValueType, Scan, UnpackValue>;

    using TBase::SetStringValue;
    using TBase::OffsetsReader_;
    using TBase::StringData_;
    using typename TBase::TOffsetsReader;

    void InitDirectReader(const char* ptr)
    {
        OffsetsReader_ = TOffsetsReader(reinterpret_cast<const ui64*>(ptr));
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
    , public TDirectStringValueExtractorBase<ValueType, true, false>
{
public:
    TDirectDenseVersionedStringValueExtractor(TRef data, const TSegmentMeta& meta, bool aggregate)
        : TDenseVersionedValueExtractorBase(meta, aggregate)
        , TDirectStringValueExtractorBase<ValueType, true, false>(meta)
    {
        const char* ptr = data.Begin();
        ptr += InitDenseReader(ptr);
        TDirectStringValueExtractorBase<ValueType, true, false>::InitDirectReader(ptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TDictionaryDenseVersionedStringValueExtractor
    : public TDenseVersionedValueExtractorBase
    , public TDictionaryStringValueExtractorBase<ValueType, true, false>
{
public:
    TDictionaryDenseVersionedStringValueExtractor(TRef data, const TSegmentMeta& meta, bool aggregate)
        : TDenseVersionedValueExtractorBase(meta, aggregate)
        , TDictionaryStringValueExtractorBase<ValueType, true, false>(meta)
    {
        const char* ptr = data.Begin();
        ptr += InitDenseReader(ptr);
        TDictionaryStringValueExtractorBase<ValueType, true, false>::InitDictionaryReader(ptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TDirectSparseVersionedStringValueExtractor
    : public TSparseVersionedValueExtractorBase
    , public TDirectStringValueExtractorBase<ValueType, true, false>
{
public:
    TDirectSparseVersionedStringValueExtractor(TRef data, const TSegmentMeta& meta, bool aggregate)
        : TSparseVersionedValueExtractorBase(meta, aggregate)
        , TDirectStringValueExtractorBase<ValueType, true, false>(meta)
    {
        const char* ptr = data.Begin();
        ptr += InitSparseReader(ptr);
        TDirectStringValueExtractorBase<ValueType, true, false>::InitDirectReader(ptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TDictionarySparseVersionedStringValueExtractor
    : public TSparseVersionedValueExtractorBase
    , public TDictionaryStringValueExtractorBase<ValueType, true, false>
{
public:
    TDictionarySparseVersionedStringValueExtractor(TRef data, const TSegmentMeta& meta, bool aggregate)
        : TSparseVersionedValueExtractorBase(meta, aggregate)
        , TDictionaryStringValueExtractorBase<ValueType, true, false>(meta)
    {
        const char* ptr = data.Begin();
        ptr += InitSparseReader(ptr);
        TDictionaryStringValueExtractorBase<ValueType, true, false>::InitDictionaryReader(ptr);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan = true>
class TDirectRleStringUnversionedValueExtractor
    : public TRleValueExtractorBase<Scan>
    , public TDirectStringValueExtractorBase<ValueType, Scan, true>
{
public:
    TDirectRleStringUnversionedValueExtractor(TRef data, const TSegmentMeta& meta)
        : TBase(meta)
    {
        const char* ptr = data.Begin();
        RowIndexReader_ = TRowIndexReader(reinterpret_cast<const ui64*>(ptr));
        ptr += RowIndexReader_.GetByteSize();

        TBase::InitDirectReader(ptr);
    }

private:
    using TBase = TDirectStringValueExtractorBase<ValueType, Scan, true>;

    using TRleValueExtractorBase<Scan>::RowIndexReader_;
    using typename TRleValueExtractorBase<Scan>::TRowIndexReader;
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan = true>
class TDictionaryRleStringUnversionedValueExtractor
    : public TRleValueExtractorBase<Scan>
    , public TDictionaryStringValueExtractorBase<ValueType, Scan, true>
{
public:
    TDictionaryRleStringUnversionedValueExtractor(TRef data, const TSegmentMeta& meta)
        : TBase(meta)
    {
        const char* ptr = data.Begin();
        RowIndexReader_ = TRowIndexReader(reinterpret_cast<const ui64*>(ptr));
        ptr += RowIndexReader_.GetByteSize();

        TBase::InitDictionaryReader(ptr);
    }

private:
    using TBase = TDictionaryStringValueExtractorBase<ValueType, Scan, true>;

    using TRleValueExtractorBase<Scan>::RowIndexReader_;
    using typename TRleValueExtractorBase<Scan>::TRowIndexReader;
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan>
class TDictionaryDenseStringUnversionedValueExtractor
    : public TDictionaryStringValueExtractorBase<ValueType, Scan, true>
{
public:
    TDictionaryDenseStringUnversionedValueExtractor(TRef data, const TSegmentMeta& meta)
        : TBase(meta)
    {
        const char* ptr = data.Begin();
        TBase::InitDictionaryReader(ptr);
    }

private:
    using TBase = TDictionaryStringValueExtractorBase<ValueType, Scan, true>;
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan>
class TDirectDenseStringUnversionedValueExtractor
    : public TDirectStringValueExtractorBase<ValueType, Scan, true>
{
public:
    TDirectDenseStringUnversionedValueExtractor(TRef data, const TSegmentMeta& meta)
        : TBase(meta)
    {
        TBase::InitDirectReader(data.Begin());
        YCHECK(meta.row_count() == OffsetsReader_.GetSize());
    }

private:
    using TBase = TDirectStringValueExtractorBase<ValueType, Scan, true>;
    using TBase::OffsetsReader_;
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
                Y_UNREACHABLE();
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

template <EValueType ValueType>
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
        return DoGetEqualRange<ValueType>(
            value, 
            lowerRowIndex, 
            upperRowIndex);
    }

private:
    virtual std::unique_ptr<IUnversionedSegmentReader> CreateSegmentReader(int segmentIndex, bool scan) override
    {
        typedef TDenseUnversionedSegmentReader<
            ValueType,
            TDirectDenseStringUnversionedValueExtractor<ValueType, true>> TDirectDenseScanReader;

        typedef TDenseUnversionedSegmentReader<
            ValueType,
            TDirectDenseStringUnversionedValueExtractor<ValueType, false>> TDirectDenseLookupReader;

        typedef TDenseUnversionedSegmentReader<
            ValueType,
            TDictionaryDenseStringUnversionedValueExtractor<ValueType, true>> TDictionaryDenseScanReader;

        typedef TDenseUnversionedSegmentReader<
            ValueType,
            TDictionaryDenseStringUnversionedValueExtractor<ValueType, false>> TDictionaryDenseLookupReader;

        typedef TRleUnversionedSegmentReader<
            ValueType,
            TDirectRleStringUnversionedValueExtractor<ValueType, true>> TDirectRleScanReader;

        typedef TRleUnversionedSegmentReader<
            ValueType,
            TDirectRleStringUnversionedValueExtractor<ValueType, false>> TDirectRleLookupReader;

        typedef TRleUnversionedSegmentReader<
            ValueType,
            TDictionaryRleStringUnversionedValueExtractor<ValueType, true>> TDictionaryRleScanReader;

        typedef TRleUnversionedSegmentReader<
            ValueType,
            TDictionaryRleStringUnversionedValueExtractor<ValueType, false>> TDictionaryRleLookupReader;

        const auto& meta = ColumnMeta_.segments(segmentIndex);
        auto segmentType = EUnversionedStringSegmentType(meta.type());

        switch (segmentType) {
            case EUnversionedStringSegmentType::DirectDense:
                if (scan) {
                    return DoCreateSegmentReader<TDirectDenseScanReader>(meta);
                } else {
                    return DoCreateSegmentReader<TDirectDenseLookupReader>(meta);
                }

            case EUnversionedStringSegmentType::DictionaryDense:
                if (scan) {
                    return DoCreateSegmentReader<TDictionaryDenseScanReader>(meta);
                } else {
                    return DoCreateSegmentReader<TDictionaryDenseLookupReader>(meta);
                }

            case EUnversionedStringSegmentType::DirectRle:
                if (scan) {
                    return DoCreateSegmentReader<TDirectRleScanReader>(meta);
                } else {
                    return DoCreateSegmentReader<TDirectRleLookupReader>(meta);
                }

            case EUnversionedStringSegmentType::DictionaryRle:
                if (scan) {
                    return DoCreateSegmentReader<TDictionaryRleScanReader>(meta);
                } else {
                    return DoCreateSegmentReader<TDictionaryRleLookupReader>(meta);
                }

            default:
                Y_UNREACHABLE();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedStringColumnReader(
    const TColumnMeta& columnMeta,
    int columnIndex,
    int columnId)
{
    return std::make_unique<TUnversionedStringColumnReader<EValueType::String>>(
        columnMeta,
        columnIndex,
        columnId);
}

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedAnyColumnReader(
    const TColumnMeta& columnMeta,
    int columnIndex,
    int columnId)
{
    return std::make_unique<TUnversionedStringColumnReader<EValueType::Any>>(
        columnMeta,
        columnIndex,
        columnId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableChunkFormat
} // namespace NYT
