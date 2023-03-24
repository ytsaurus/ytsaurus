#include "string_column_reader.h"

#include "column_reader_detail.h"
#include "private.h"
#include "helpers.h"

#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/yson/lexer.h>

#include <yt/yt/core/misc/bitmap.h>
#include <yt/yt/core/misc/bit_packed_unsigned_vector.h>

#include <library/cpp/yt/coding/zig_zag.h>

namespace NYT::NTableChunkFormat {

using namespace NTableClient;
using namespace NProto;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan, bool UnpackValue>
class TStringValueExtractorBase
{
protected:
    const NProto::TStringSegmentMeta& StringMeta_;

    mutable TStatelessLexer Lexer_;
    using TOffsetsReader = TBitPackedUnsignedVectorReader<ui32, Scan>;
    TOffsetsReader OffsetReader_;
    TRef StringData_;

    explicit TStringValueExtractorBase(const TSegmentMeta& segmentMeta)
        : StringMeta_(segmentMeta.GetExtension(TStringSegmentMeta::string_segment_meta))
    { }

    ui32 GetOffset(i64 offsetIndex) const
    {
        return StringMeta_.expected_length() * (offsetIndex + 1) +
            ZigZagDecode32(OffsetReader_[offsetIndex]);
    }

    void SetStringValue(TUnversionedValue* value, i64 offsetIndex, int id, EValueFlags flags) const
    {
        ui32 padding = offsetIndex == 0 ? 0 : GetOffset(offsetIndex - 1);
        const char* begin = StringData_.Begin() + padding;
        ui32 length = GetOffset(offsetIndex) - padding;
        auto string = TStringBuf(begin, length);

        if constexpr (ValueType == EValueType::String) {
            *value = MakeUnversionedStringValue(string, id, flags);
        } else if constexpr (ValueType == EValueType::Composite) {
            *value = MakeUnversionedCompositeValue(string, id, flags);
        } else if constexpr (ValueType == EValueType::Any) {
            if constexpr (UnpackValue) {
                YT_ASSERT(flags == EValueFlags::None);
                *value = MakeUnversionedValue(string, id, Lexer_);
            } else {
                *value = MakeUnversionedAnyValue(string, id, flags);
            }
        } else {
            // Effectively static_assert(false);
            static_assert(ValueType == EValueType::String, "Unexpected ValueType");
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

    void ExtractValue(TUnversionedValue* value, i64 valueIndex, int id, EValueFlags flags) const
    {
        auto dictionaryIndex = IndexReader_[valueIndex];
        if (dictionaryIndex == 0) {
            *value = MakeUnversionedSentinelValue(EValueType::Null, id, flags);
        } else {
            SetStringValue(value, dictionaryIndex - 1, id, flags);
        }
    }

protected:
    const IUnversionedColumnarRowBatch::TDictionaryId DictionaryId_ = IUnversionedColumnarRowBatch::GenerateDictionaryId();

    using TBase = TStringValueExtractorBase<ValueType, Scan, UnpackValue>;
    using TIndexReader = TBitPackedUnsignedVectorReader<ui32, Scan>;
    TIndexReader IndexReader_;

    using TBase::SetStringValue;
    using TBase::OffsetReader_;
    using TBase::StringData_;
    using typename TBase::TOffsetsReader;

    const char* InitDictionaryReader(const char* begin, const char* end)
    {
        const char* ptr = begin;

        IndexReader_ = TIndexReader(reinterpret_cast<const ui64*>(ptr));
        ptr += IndexReader_.GetByteSize();

        OffsetReader_ = TOffsetsReader(reinterpret_cast<const ui64*>(ptr));
        ptr += OffsetReader_.GetByteSize();

        StringData_ = TRef(ptr, end);

        return end;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan, bool UnpackValue>
class TDirectStringValueExtractorBase
    : public TStringValueExtractorBase<ValueType, Scan, UnpackValue>
{
public:
    using TStringValueExtractorBase<ValueType, Scan, UnpackValue>::TStringValueExtractorBase;

    void ExtractValue(TUnversionedValue* value, i64 valueIndex, int id, EValueFlags flags) const
    {
        if (NullBitmap_[valueIndex]) {
            *value = MakeUnversionedSentinelValue(EValueType::Null, id, flags);
        } else {
            SetStringValue(value, valueIndex, id, flags);
        }
    }

protected:
    TReadOnlyBitmap NullBitmap_;

    using TBase = TStringValueExtractorBase<ValueType, Scan, UnpackValue>;
    using TBase::SetStringValue;
    using TBase::OffsetReader_;
    using TBase::StringData_;
    using typename TBase::TOffsetsReader;

    const char* InitDirectReader(const char* begin, const char* end)
    {
        const char* ptr = begin;

        OffsetReader_ = TOffsetsReader(reinterpret_cast<const ui64*>(ptr));
        ptr += OffsetReader_.GetByteSize();

        NullBitmap_ = TReadOnlyBitmap(ptr, OffsetReader_.GetSize());
        ptr += AlignUp(NullBitmap_.GetByteSize(), SerializationAlignment);

        StringData_ = TRef(ptr, end);
        ptr += StringData_.Size();

        return ptr;
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
        ptr = InitDenseReader(ptr);
        ptr = InitDirectReader(ptr, data.End());
        YT_VERIFY(ptr == data.End());
    }

private:
    using TDirectStringValueExtractorBase<ValueType, true, false>::InitDirectReader;
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
        ptr = InitDenseReader(ptr);
        ptr = TDictionaryStringValueExtractorBase<ValueType, true, false>::InitDictionaryReader(ptr, data.End());
        YT_VERIFY(ptr == data.End());
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
        : TSparseVersionedValueExtractorBase(aggregate)
        , TDirectStringValueExtractorBase<ValueType, true, false>(meta)
    {
        const char* ptr = data.Begin();
        ptr = InitSparseReader(ptr);
        ptr = TDirectStringValueExtractorBase<ValueType, true, false>::InitDirectReader(ptr, data.End());
        YT_VERIFY(ptr == data.End());
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
        : TSparseVersionedValueExtractorBase(aggregate)
        , TDictionaryStringValueExtractorBase<ValueType, true, false>(meta)
    {
        const char* ptr = data.Begin();
        ptr = InitSparseReader(ptr);
        ptr = TDictionaryStringValueExtractorBase<ValueType, true, false>::InitDictionaryReader(ptr, data.End());
        YT_VERIFY(ptr == data.End());
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
        : TDirectStringValueExtractorBase<ValueType, Scan, true>(meta)
    {
        const char* ptr = data.Begin();

        RowIndexReader_ = TRowIndexReader(reinterpret_cast<const ui64*>(ptr));
        ptr += RowIndexReader_.GetByteSize();

        ptr = TDirectStringValueExtractorBase<ValueType, Scan, true>::InitDirectReader(ptr, data.End());

        YT_VERIFY(ptr == data.End());
    }

    int GetBatchColumnCount()
    {
        return 2;
    }

    void ReadColumnarBatch(
        i64 startRowIndex,
        i64 rowCount,
        TMutableRange<IUnversionedColumnarRowBatch::TColumn> columns)
    {
        YT_VERIFY(columns.size() == 2);
        auto& primaryColumn = columns[0];
        auto& rleColumn = columns[1];
        ReadColumnarStringValues(
            &rleColumn,
            0,
            OffsetReader_.GetSize(),
            StringMeta_.expected_length(),
            OffsetReader_.GetData(),
            StringData_);
        ReadColumnarNullBitmap(
            &rleColumn,
            -1,
            -1,
            NullBitmap_.GetData());
        ReadColumnarRle(
            &primaryColumn,
            &rleColumn,
            primaryColumn.Type,
            startRowIndex,
            rowCount,
            RowIndexReader_.GetData());
    }

private:
    using TRleValueExtractorBase<Scan>::RowIndexReader_;
    using typename TRleValueExtractorBase<Scan>::TRowIndexReader;
    using TDirectStringValueExtractorBase<ValueType, Scan, true>::OffsetReader_;
    using TDirectStringValueExtractorBase<ValueType, Scan, true>::NullBitmap_;
    using TDirectStringValueExtractorBase<ValueType, Scan, true>::StringMeta_;
    using TDirectStringValueExtractorBase<ValueType, Scan, true>::StringData_;
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan = true>
class TDictionaryRleStringUnversionedValueExtractor
    : public TRleValueExtractorBase<Scan>
    , public TDictionaryStringValueExtractorBase<ValueType, Scan, true>
{
public:
    TDictionaryRleStringUnversionedValueExtractor(TRef data, const TSegmentMeta& meta)
        : TDictionaryStringValueExtractorBase<ValueType, Scan, true>(meta)
    {
        const char* ptr = data.Begin();
        RowIndexReader_ = TRowIndexReader(reinterpret_cast<const ui64*>(ptr));
        ptr += RowIndexReader_.GetByteSize();
        ptr = TDictionaryStringValueExtractorBase<ValueType, Scan, true>::InitDictionaryReader(ptr, data.End());
        YT_VERIFY(ptr == data.End());
    }

    int GetBatchColumnCount()
    {
        return 3;
    }

    void ReadColumnarBatch(
        i64 startRowIndex,
        i64 rowCount,
        TMutableRange<IUnversionedColumnarRowBatch::TColumn> columns)
    {
        YT_VERIFY(columns.size() == 3);
        auto& primaryColumn = columns[0];
        auto& dictionaryColumn = columns[1];
        auto& rleColumn = columns[2];
        ReadColumnarStringValues(
            &dictionaryColumn,
            0,
            OffsetReader_.GetSize(),
            StringMeta_.expected_length(),
            OffsetReader_.GetData(),
            StringData_);
        ReadColumnarDictionary(
            &rleColumn,
            &dictionaryColumn,
            DictionaryId_,
            primaryColumn.Type,
            -1,
            -1,
            IndexReader_.GetData());
        ReadColumnarRle(
            &primaryColumn,
            &rleColumn,
            primaryColumn.Type,
            startRowIndex,
            rowCount,
            RowIndexReader_.GetData());
    }

private:
    using TRleValueExtractorBase<Scan>::RowIndexReader_;
    using typename TRleValueExtractorBase<Scan>::TRowIndexReader;
    using TDictionaryStringValueExtractorBase<ValueType, Scan, true>::IndexReader_;
    using TDictionaryStringValueExtractorBase<ValueType, Scan, true>::OffsetReader_;
    using TDictionaryStringValueExtractorBase<ValueType, Scan, true>::StringMeta_;
    using TDictionaryStringValueExtractorBase<ValueType, Scan, true>::StringData_;
    using TDictionaryStringValueExtractorBase<ValueType, Scan, true>::DictionaryId_;
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
        ptr = TBase::InitDictionaryReader(data.Begin(), data.End());
        YT_VERIFY(ptr == data.End());
    }

    int GetBatchColumnCount()
    {
        return 2;
    }

    void ReadColumnarBatch(
        i64 startRowIndex,
        i64 rowCount,
        TMutableRange<IUnversionedColumnarRowBatch::TColumn> columns)
    {
        YT_VERIFY(columns.size() == 2);
        auto& primaryColumn = columns[0];
        auto& dictionaryColumn = columns[1];
        ReadColumnarStringValues(
            &dictionaryColumn,
            0,
            OffsetReader_.GetSize(),
            StringMeta_.expected_length(),
            OffsetReader_.GetData(),
            StringData_);
        ReadColumnarDictionary(
            &primaryColumn,
            &dictionaryColumn,
            DictionaryId_,
            primaryColumn.Type,
            startRowIndex,
            rowCount,
            IndexReader_.GetData());
    }

private:
    using TBase = TDictionaryStringValueExtractorBase<ValueType, Scan, true>;
    using TBase::IndexReader_;
    using TBase::OffsetReader_;
    using TBase::StringMeta_;
    using TBase::StringData_;
    using TBase::DictionaryId_;
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
        const char* ptr = data.Begin();
        ptr = TBase::InitDirectReader(ptr, data.End());
        YT_VERIFY(ptr == data.End());
        YT_VERIFY(meta.row_count() == static_cast<i64>(OffsetReader_.GetSize()));
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
        ReadColumnarStringValues(
            &column,
            startRowIndex,
            rowCount,
            StringMeta_.expected_length(),
            OffsetReader_.GetData(),
            StringData_);
        ReadColumnarNullBitmap(
            &column,
            startRowIndex,
            rowCount,
            NullBitmap_.GetData());
    }

private:
    using TBase = TDirectStringValueExtractorBase<ValueType, Scan, true>;
    using TBase::OffsetReader_;
    using TBase::NullBitmap_;
    using TBase::StringMeta_;
    using TBase::StringData_;
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TVersionedStringColumnReader
    : public TVersionedColumnReaderBase
{
public:
    TVersionedStringColumnReader(
        const TColumnMeta& columnMeta,
        int columnId,
        const TColumnSchema& columnSchema)
        : TVersionedColumnReaderBase(
            columnMeta,
            columnId,
            columnSchema)
    { }

private:
    std::unique_ptr<IVersionedSegmentReader> CreateSegmentReader(int segmentIndex) override
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
        auto segmentType = FromProto<EVersionedStringSegmentType>(meta.type());
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
                YT_ABORT();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedColumnReader> CreateVersionedStringColumnReader(
    const TColumnMeta& columnMeta,
    int columnId,
    const TColumnSchema& columnSchema)
{
    return std::make_unique<TVersionedStringColumnReader<EValueType::String>>(
        columnMeta,
        columnId,
        columnSchema);
}

std::unique_ptr<IVersionedColumnReader> CreateVersionedAnyColumnReader(
    const TColumnMeta& columnMeta,
    int columnId,
    const TColumnSchema& columnSchema)
{
    return std::make_unique<TVersionedStringColumnReader<EValueType::Any>>(
        columnMeta,
        columnId,
        columnSchema);
}

std::unique_ptr<IVersionedColumnReader> CreateVersionedCompositeColumnReader(
    const TColumnMeta& columnMeta,
    int columnId,
    const TColumnSchema& columnSchema)
{
    return std::make_unique<TVersionedStringColumnReader<EValueType::Composite>>(
        columnMeta,
        columnId,
        columnSchema);
}

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TUnversionedStringColumnReader
    : public TUnversionedColumnReaderBase
{
public:
    using TUnversionedColumnReaderBase::TUnversionedColumnReaderBase;

    std::pair<i64, i64> GetEqualRange(
        const TUnversionedValue& value,
        i64 lowerRowIndex,
        i64 upperRowIndex) override
    {
        return DoGetEqualRange<ValueType>(
            value,
            lowerRowIndex,
            upperRowIndex);
    }

    i64 EstimateDataWeight(
        i64 lowerRowIndex,
        i64 upperRowIndex) override
    {
        const auto& stringMeta = CurrentSegmentMeta().GetExtension(TStringSegmentMeta::string_segment_meta);
        return std::max<i64>(1, stringMeta.expected_length()) * (upperRowIndex - lowerRowIndex);
    }

private:
    std::unique_ptr<IUnversionedSegmentReader> CreateSegmentReader(int segmentIndex, bool scan) override
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
        auto segmentType = FromProto<EUnversionedStringSegmentType>(meta.type());
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
                YT_ABORT();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedStringColumnReader(
    const TColumnMeta& columnMeta,
    int columnIndex,
    int columnId,
    std::optional<ESortOrder> sortOrder,
    const TColumnSchema& columnSchema)
{
    return std::make_unique<TUnversionedStringColumnReader<EValueType::String>>(
        columnMeta,
        columnIndex,
        columnId,
        sortOrder,
        columnSchema);
}

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedAnyColumnReader(
    const TColumnMeta& columnMeta,
    int columnIndex,
    int columnId,
    std::optional<ESortOrder> sortOrder,
    const TColumnSchema& columnSchema)
{
    return std::make_unique<TUnversionedStringColumnReader<EValueType::Any>>(
        columnMeta,
        columnIndex,
        columnId,
        sortOrder,
        columnSchema);
}

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedCompositeColumnReader(
    const TColumnMeta& columnMeta,
    int columnIndex,
    int columnId,
    std::optional<ESortOrder> sortOrder,
    const TColumnSchema& columnSchema)
{
    return std::make_unique<TUnversionedStringColumnReader<EValueType::Composite>>(
        columnMeta,
        columnIndex,
        columnId,
        sortOrder,
        columnSchema);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
