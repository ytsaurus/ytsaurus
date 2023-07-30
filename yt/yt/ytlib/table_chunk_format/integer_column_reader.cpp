#include "integer_column_reader.h"

#include "column_reader_detail.h"
#include "private.h"

#include <yt/yt/core/misc/bit_packed_unsigned_vector.h>

namespace NYT::NTableChunkFormat {

using namespace NProto;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
void SetIntegerValue(TUnversionedValue* value, ui64 data, int id, EValueFlags flags);

template <>
void SetIntegerValue<EValueType::Int64>(TUnversionedValue* value, ui64 data, int id, EValueFlags flags)
{
    *value = MakeUnversionedInt64Value(ZigZagDecode64(data), id, flags);
}

template <>
void SetIntegerValue<EValueType::Uint64>(TUnversionedValue* value, ui64 data, int id, EValueFlags flags)
{
    *value = MakeUnversionedUint64Value(data, id, flags);
}

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan>
class TIntegerValueExtractorBase
{
protected:
    const TIntegerSegmentMeta& Meta_;

    using TValueReader = TBitPackedUnsignedVectorReader<ui64, Scan>;
    TValueReader ValueReader_;

    explicit TIntegerValueExtractorBase(const TSegmentMeta& meta)
        : Meta_(meta.GetExtension(TIntegerSegmentMeta::integer_segment_meta))
    { }

    void SetValue(TUnversionedValue* value, i64 valueIndex, int id, EValueFlags flags) const
    {
        SetIntegerValue<ValueType>(value, Meta_.min_value() + ValueReader_[valueIndex], id, flags);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan>
class TDirectIntegerValueExtractorBase
    : public TIntegerValueExtractorBase<ValueType, Scan>
{
public:
    using TIntegerValueExtractorBase<ValueType, Scan>::TIntegerValueExtractorBase;

    void ExtractValue(TUnversionedValue* value, i64 valueIndex, int id, EValueFlags flags) const
    {
        YT_ASSERT(None(flags & EValueFlags::Hunk));
        if (NullBitmap_[valueIndex]) {
            *value = MakeUnversionedSentinelValue(EValueType::Null, id, flags);
        } else {
            TIntegerValueExtractorBase<ValueType, Scan>::SetValue(value, valueIndex, id, flags);
        }
    }

protected:
    TReadOnlyBitmap NullBitmap_;

    using TIntegerValueExtractorBase<ValueType, Scan>::ValueReader_;
    using typename TIntegerValueExtractorBase<ValueType, Scan>::TValueReader;

    const char* InitDirectReader(const char* ptr)
    {
        ValueReader_ = TValueReader(reinterpret_cast<const ui64*>(ptr));
        ptr += ValueReader_.GetByteSize();

        NullBitmap_ = TReadOnlyBitmap(ptr, ValueReader_.GetSize());
        ptr += AlignUp(NullBitmap_.GetByteSize(), SerializationAlignment);

        return ptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan>
class TDictionaryIntegerValueExtractorBase
    : public TIntegerValueExtractorBase<ValueType, Scan>
{
public:
    using TIntegerValueExtractorBase<ValueType, Scan>::TIntegerValueExtractorBase;

    void ExtractValue(TUnversionedValue* value, i64 valueIndex, int id, EValueFlags flags) const
    {
        YT_ASSERT(None(flags & EValueFlags::Hunk));
        auto dictionaryId = IndexReader_[valueIndex];
        if (dictionaryId == 0) {
            *value = MakeUnversionedSentinelValue(EValueType::Null, id, flags);
        } else {
            TIntegerValueExtractorBase<ValueType, Scan>::SetValue(value, dictionaryId - 1, id, flags);
        }
    }

protected:
    const IUnversionedColumnarRowBatch::TDictionaryId DictionaryId_ = IUnversionedColumnarRowBatch::GenerateDictionaryId();

    using TIndexReader = TBitPackedUnsignedVectorReader<ui32, Scan>;
    TIndexReader IndexReader_;

    using TIntegerValueExtractorBase<ValueType, Scan>::ValueReader_;
    using typename TIntegerValueExtractorBase<ValueType, Scan>::TValueReader;

    const char* InitDictionaryReader(const char* ptr)
    {
        ValueReader_ = TValueReader(reinterpret_cast<const ui64*>(ptr));
        ptr += ValueReader_.GetByteSize();

        IndexReader_ = TIndexReader(reinterpret_cast<const ui64*>(ptr));
        ptr += IndexReader_.GetByteSize();

        return ptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TDirectDenseVersionedIntegerValueExtractor
    : public TDenseVersionedValueExtractorBase
    , public TDirectIntegerValueExtractorBase<ValueType, true>
{
public:
    TDirectDenseVersionedIntegerValueExtractor(
        TRef data,
        const TSegmentMeta& meta,
        bool aggregate)
        : TDenseVersionedValueExtractorBase(meta, aggregate)
        , TDirectIntegerValueExtractorBase<ValueType, true>(meta)
    {
        const char* ptr = data.Begin();
        ptr = TDenseVersionedValueExtractorBase::InitDenseReader(ptr);
        ptr = TDirectIntegerValueExtractorBase<ValueType, true>::InitDirectReader(ptr);
        YT_VERIFY(ptr == data.End());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TDictionaryDenseVersionedIntegerValueExtractor
    : public TDenseVersionedValueExtractorBase
    , public TDictionaryIntegerValueExtractorBase<ValueType, true>
{
public:
    TDictionaryDenseVersionedIntegerValueExtractor(
        TRef data,
        const TSegmentMeta& meta,
        bool aggregate)
        : TDenseVersionedValueExtractorBase(meta, aggregate)
        , TDictionaryIntegerValueExtractorBase<ValueType, true>(meta)
    {
        const char* ptr = data.Begin();
        ptr = TDenseVersionedValueExtractorBase::InitDenseReader(ptr);
        ptr = TDictionaryIntegerValueExtractorBase<ValueType, true>::InitDictionaryReader(ptr);
        YT_VERIFY(ptr == data.End());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TDirectSparseVersionedIntegerValueExtractor
    : public TSparseVersionedValueExtractorBase
    , public TDirectIntegerValueExtractorBase<ValueType, true>
{
public:
    TDirectSparseVersionedIntegerValueExtractor(
        TRef data,
        const TSegmentMeta& meta,
        bool aggregate)
        : TSparseVersionedValueExtractorBase(aggregate)
        , TDirectIntegerValueExtractorBase<ValueType, true>(meta)
    {
        const char* ptr = data.Begin();
        ptr = TSparseVersionedValueExtractorBase::InitSparseReader(ptr);
        ptr = TDirectIntegerValueExtractorBase<ValueType, true>::InitDirectReader(ptr);
        YT_VERIFY(ptr == data.End());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TDictionarySparseVersionedIntegerValueExtractor
    : public TSparseVersionedValueExtractorBase
    , public TDictionaryIntegerValueExtractorBase<ValueType, true>
{
public:
    TDictionarySparseVersionedIntegerValueExtractor(
        TRef data,
        const TSegmentMeta& meta,
        bool aggregate)
        : TSparseVersionedValueExtractorBase(aggregate)
        , TDictionaryIntegerValueExtractorBase<ValueType, true>(meta)
    {
        const char* ptr = data.Begin();
        ptr = TSparseVersionedValueExtractorBase::InitSparseReader(ptr);
        ptr = TDictionaryIntegerValueExtractorBase<ValueType, true>::InitDictionaryReader(ptr);
        YT_VERIFY(ptr == data.End());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TVersionedIntegerColumnReader
    : public TVersionedColumnReaderBase
{
public:
    using TVersionedColumnReaderBase::TVersionedColumnReaderBase;

private:
    std::unique_ptr<IVersionedSegmentReader> CreateSegmentReader(int segmentIndex) override
    {
        using TDirectDenseReader = TDenseVersionedSegmentReader<
            TDirectDenseVersionedIntegerValueExtractor<ValueType>>;
        using TDictionaryDenseReader = TDenseVersionedSegmentReader<
            TDictionaryDenseVersionedIntegerValueExtractor<ValueType>>;
        using TDirectSparseReader = TSparseVersionedSegmentReader<
            TDirectSparseVersionedIntegerValueExtractor<ValueType>>;
        using TDictionarySparseReader = TSparseVersionedSegmentReader<
            TDictionarySparseVersionedIntegerValueExtractor<ValueType>>;

        const auto& meta = ColumnMeta_.segments(segmentIndex);
        auto segmentType = EVersionedIntegerSegmentType(meta.type());

        switch (segmentType) {
            case EVersionedIntegerSegmentType::DirectDense:
                return DoCreateSegmentReader<TDirectDenseReader>(meta);

            case EVersionedIntegerSegmentType::DictionaryDense:
                return DoCreateSegmentReader<TDictionaryDenseReader>(meta);

            case EVersionedIntegerSegmentType::DirectSparse:
                return DoCreateSegmentReader<TDirectSparseReader>(meta);

            case EVersionedIntegerSegmentType::DictionarySparse:
                return DoCreateSegmentReader<TDictionarySparseReader>(meta);

            default:
                YT_ABORT();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedColumnReader> CreateVersionedInt64ColumnReader(
    const TColumnMeta& columnMeta,
    int columnId,
    const TColumnSchema& columnSchema)
{
    return std::make_unique<TVersionedIntegerColumnReader<EValueType::Int64>>(
        columnMeta,
        columnId,
        columnSchema);
}

std::unique_ptr<IVersionedColumnReader> CreateVersionedUint64ColumnReader(
    const TColumnMeta& columnMeta,
    int columnId,
    const TColumnSchema& columnSchema)
{
    return std::make_unique<TVersionedIntegerColumnReader<EValueType::Uint64>>(
        columnMeta,
        columnId,
        columnSchema);
}

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan>
class TDirectDenseUnversionedIntegerValueExtractor
    : public TDirectIntegerValueExtractorBase<ValueType, Scan>
{
public:
    TDirectDenseUnversionedIntegerValueExtractor(
        TRef data,
        const TSegmentMeta& meta)
        : TDirectIntegerValueExtractorBase<ValueType, Scan>(meta)
    {
        const char* ptr = data.Begin();
        ptr = InitDirectReader(ptr);
        YT_VERIFY(ptr == data.End());
        YT_VERIFY(static_cast<i64>(ValueReader_.GetSize()) == meta.row_count());
    }

    int GetBatchColumnCount() const
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
        ReadColumnarIntegerValues(
            &column,
            startRowIndex,
            rowCount,
            ValueType,
            Meta_.min_value(),
            ValueReader_.GetData());
        ReadColumnarNullBitmap(
            &column,
            startRowIndex,
            rowCount,
            NullBitmap_.GetData());
    }

private:
    using TDirectIntegerValueExtractorBase<ValueType, Scan>::ValueReader_;
    using TDirectIntegerValueExtractorBase<ValueType, Scan>::NullBitmap_;
    using TDirectIntegerValueExtractorBase<ValueType, Scan>::Meta_;
    using TDirectIntegerValueExtractorBase<ValueType, Scan>::InitDirectReader;
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan>
class TDictionaryDenseUnversionedIntegerValueExtractor
    : public TDictionaryIntegerValueExtractorBase<ValueType, Scan>
{
public:
    TDictionaryDenseUnversionedIntegerValueExtractor(
        TRef data,
        const TSegmentMeta& meta)
        : TDictionaryIntegerValueExtractorBase<ValueType, Scan>(meta)
    {
        const char* ptr = data.Begin();
        ptr = TDictionaryIntegerValueExtractorBase<ValueType, Scan>::InitDictionaryReader(ptr);
        YT_VERIFY(ptr == data.End());
    }

    int GetBatchColumnCount()
    {
        return 2;
    }

    void ReadColumnarBatch(
        i64 startRowIndex,
        i64 rowCount,
        TMutableRange<NTableClient::IUnversionedColumnarRowBatch::TColumn> columns)
    {
        YT_VERIFY(columns.size() == 2);
        auto& primaryColumn = columns[0];
        auto& dictionaryColumn = columns[1];
        ReadColumnarIntegerValues(
            &dictionaryColumn,
            0,
            ValueReader_.GetSize(),
            ValueType,
            Meta_.min_value(),
            ValueReader_.GetData());
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
    using TDictionaryIntegerValueExtractorBase<ValueType, Scan>::IndexReader_;
    using TDictionaryIntegerValueExtractorBase<ValueType, Scan>::ValueReader_;
    using TDictionaryIntegerValueExtractorBase<ValueType, Scan>::Meta_;
    using TDictionaryIntegerValueExtractorBase<ValueType, Scan>::DictionaryId_;
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan>
class TDirectRleUnversionedIntegerValueExtractor
    : public TDirectIntegerValueExtractorBase<ValueType, Scan>
    , public TRleValueExtractorBase<Scan>
{
public:
    TDirectRleUnversionedIntegerValueExtractor(
        TRef data,
        const TSegmentMeta& meta)
        : TDirectIntegerValueExtractorBase<ValueType, Scan>(meta)
    {
        const char* ptr = data.Begin();
        ptr = TDirectIntegerValueExtractorBase<ValueType, Scan>::InitDirectReader(ptr);
        RowIndexReader_ = TRowIndexReader(reinterpret_cast<const ui64*>(ptr));
        ptr += RowIndexReader_.GetByteSize();
        YT_VERIFY(ptr == data.End());
    }

    int GetBatchColumnCount()
    {
        return 2;
    }

    void ReadColumnarBatch(
        i64 startRowIndex,
        i64 rowCount,
        TMutableRange<NTableClient::IUnversionedColumnarRowBatch::TColumn> columns)
    {
        YT_VERIFY(columns.size() == 2);
        auto& primaryColumn = columns[0];
        auto& rleColumn = columns[1];
        ReadColumnarIntegerValues(
            &rleColumn,
            -1,
            -1,
            ValueType,
            Meta_.min_value(),
            ValueReader_.GetData());
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
    using typename TRleValueExtractorBase<Scan>::TRowIndexReader;
    using TDirectIntegerValueExtractorBase<ValueType, Scan>::ValueReader_;
    using TDirectIntegerValueExtractorBase<ValueType, Scan>::NullBitmap_;
    using TDirectIntegerValueExtractorBase<ValueType, Scan>::Meta_;
    using TRleValueExtractorBase<Scan>::RowIndexReader_;
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan>
class TDictionaryRleUnversionedIntegerValueExtractor
    : public TDictionaryIntegerValueExtractorBase<ValueType, Scan>
    , public TRleValueExtractorBase<Scan>
{
public:
    TDictionaryRleUnversionedIntegerValueExtractor(
        TRef data,
        const TSegmentMeta& meta)
        : TDictionaryIntegerValueExtractorBase<ValueType, Scan>(meta)
    {
        const char* ptr = data.Begin();
        ptr = TDictionaryIntegerValueExtractorBase<ValueType, Scan>::InitDictionaryReader(ptr);
        RowIndexReader_ = TRowIndexReader(reinterpret_cast<const ui64*>(ptr));
        ptr += RowIndexReader_.GetByteSize();
        YT_VERIFY(ptr == data.End());
    }

    int GetBatchColumnCount()
    {
        return 3;
    }

    void ReadColumnarBatch(
        i64 startRowIndex,
        i64 rowCount,
        TMutableRange<NTableClient::IUnversionedColumnarRowBatch::TColumn> columns)
    {
        YT_VERIFY(columns.size() == 3);
        auto& primaryColumn = columns[0];
        auto& dictionaryColumn = columns[1];
        auto& rleColumn = columns[2];
        ReadColumnarIntegerValues(
            &dictionaryColumn,
            0,
            ValueReader_.GetSize(),
            ValueType,
            Meta_.min_value(),
            ValueReader_.GetData());
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
    using typename TRleValueExtractorBase<Scan>::TRowIndexReader;
    using TDictionaryIntegerValueExtractorBase<ValueType, Scan>::ValueReader_;
    using TDictionaryIntegerValueExtractorBase<ValueType, Scan>::IndexReader_;
    using TDictionaryIntegerValueExtractorBase<ValueType, Scan>::Meta_;
    using TDictionaryIntegerValueExtractorBase<ValueType, Scan>::DictionaryId_;
    using TRleValueExtractorBase<Scan>::RowIndexReader_;
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TUnversionedIntegerColumnReader
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

private:
    std::unique_ptr<IUnversionedSegmentReader> CreateSegmentReader(int segmentIndex, bool scan) override
    {
        typedef TDenseUnversionedSegmentReader<
            ValueType,
            TDirectDenseUnversionedIntegerValueExtractor<ValueType, true>> TDirectDenseScanReader;

        typedef TDenseUnversionedSegmentReader<
            ValueType,
            TDirectDenseUnversionedIntegerValueExtractor<ValueType, false>> TDirectDenseLookupReader;

        typedef TDenseUnversionedSegmentReader<
            ValueType,
            TDictionaryDenseUnversionedIntegerValueExtractor<ValueType, true>> TDictionaryDenseScanReader;

        typedef TDenseUnversionedSegmentReader<
            ValueType,
            TDictionaryDenseUnversionedIntegerValueExtractor<ValueType, false>> TDictionaryDenseLookupReader;

        typedef TRleUnversionedSegmentReader<
            ValueType,
            TDirectRleUnversionedIntegerValueExtractor<ValueType, true>> TDirectRleScanReader;

        typedef TRleUnversionedSegmentReader<
            ValueType,
            TDirectRleUnversionedIntegerValueExtractor<ValueType, false>> TDirectRleLookupReader;

        typedef TRleUnversionedSegmentReader<
            ValueType,
            TDictionaryRleUnversionedIntegerValueExtractor<ValueType, true>> TDictionaryRleScanReader;

        typedef TRleUnversionedSegmentReader<
            ValueType,
            TDictionaryRleUnversionedIntegerValueExtractor<ValueType, false>> TDictionaryRleLookupReader;

        const auto& meta = ColumnMeta_.segments(segmentIndex);
        auto segmentType = FromProto<EUnversionedIntegerSegmentType>(meta.type());
        switch (segmentType) {
            case EUnversionedIntegerSegmentType::DirectDense:
                if (scan) {
                    return DoCreateSegmentReader<TDirectDenseScanReader>(meta);
                } else {
                    return DoCreateSegmentReader<TDirectDenseLookupReader>(meta);
                }

            case EUnversionedIntegerSegmentType::DictionaryDense:
                if (scan) {
                    return DoCreateSegmentReader<TDictionaryDenseScanReader>(meta);
                } else {
                    return DoCreateSegmentReader<TDictionaryDenseLookupReader>(meta);
                }

            case EUnversionedIntegerSegmentType::DirectRle:
                if (scan) {
                    return DoCreateSegmentReader<TDirectRleScanReader>(meta);
                } else {
                    return DoCreateSegmentReader<TDirectRleLookupReader>(meta);
                }

            case EUnversionedIntegerSegmentType::DictionaryRle:
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

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedInt64ColumnReader(
    const TColumnMeta& columnMeta,
    int columnIndex,
    int columnId,
    std::optional<ESortOrder> sortOrder,
    const NTableClient::TColumnSchema& columnSchema)
{
    return std::make_unique<TUnversionedIntegerColumnReader<EValueType::Int64>>(
        columnMeta,
        columnIndex,
        columnId,
        sortOrder,
        columnSchema);
}

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedUint64ColumnReader(
    const TColumnMeta& columnMeta,
    int columnIndex,
    int columnId,
    std::optional<ESortOrder> sortOrder,
    const NTableClient::TColumnSchema& columnSchema)
{
    return std::make_unique<TUnversionedIntegerColumnReader<EValueType::Uint64>>(
        columnMeta,
        columnIndex,
        columnId,
        sortOrder,
        columnSchema);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
