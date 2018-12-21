#include "integer_column_reader.h"

#include "compressed_integer_vector.h"
#include "column_reader_detail.h"
#include "private.h"

namespace NYT::NTableChunkFormat {

using namespace NProto;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
void SetIntegerValue(TUnversionedValue* value, ui64 data, int id, bool aggregate);

template <>
void SetIntegerValue<EValueType::Int64>(TUnversionedValue* value, ui64 data, int id, bool aggregate)
{
    *value = MakeUnversionedInt64Value(ZigZagDecode64(data), id, aggregate);
}

template <>
void SetIntegerValue<EValueType::Uint64>(TUnversionedValue* value, ui64 data, int id, bool aggregate)
{
    *value = MakeUnversionedUint64Value(data, id, aggregate);
}

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan>
class TIntegerValueExtractorBase
{
protected:
    using TValueReader = TCompressedUnsignedVectorReader<ui64, Scan>;
    TValueReader ValueReader_;

    const TIntegerSegmentMeta& Meta_;

    TIntegerValueExtractorBase(const TSegmentMeta& meta)
        : Meta_(meta.GetExtension(TIntegerSegmentMeta::integer_segment_meta))
    { }

    void SetValue(TUnversionedValue* value, i64 valueIndex, int id, bool aggregate) const
    {
        SetIntegerValue<ValueType>(value, Meta_.min_value() + ValueReader_[valueIndex], id, aggregate);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan>
class TDirectIntegerValueExtractorBase
    : public TIntegerValueExtractorBase<ValueType, Scan>
{
public:
    using TIntegerValueExtractorBase<ValueType, Scan>::TIntegerValueExtractorBase;

    void ExtractValue(TUnversionedValue* value, i64 valueIndex, int id, bool aggregate) const
    {
        if (NullBitmap_[valueIndex]) {
            *value = MakeUnversionedSentinelValue(EValueType::Null, id, aggregate);
        } else {
            TIntegerValueExtractorBase<ValueType, Scan>::SetValue(value, valueIndex, id, aggregate);
        }
    }

protected:
    TReadOnlyBitmap<ui64> NullBitmap_;

    using TIntegerValueExtractorBase<ValueType, Scan>::ValueReader_;
    using typename TIntegerValueExtractorBase<ValueType, Scan>::TValueReader;

    size_t InitDirectReader(const char* ptr)
    {
        const char* begin = ptr;

        ValueReader_ = TValueReader(reinterpret_cast<const ui64*>(ptr));
        ptr += ValueReader_.GetByteSize();

        NullBitmap_ = TReadOnlyBitmap<ui64>(
            reinterpret_cast<const ui64*>(ptr),
            ValueReader_.GetSize());
        ptr += NullBitmap_.GetByteSize();

        return ptr - begin;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan>
class TDictionaryIntegerValueExtractorBase
    : public TIntegerValueExtractorBase<ValueType, Scan>
{
public:
    using TIntegerValueExtractorBase<ValueType, Scan>::TIntegerValueExtractorBase;

    void ExtractValue(TUnversionedValue* value, i64 valueIndex, int id, bool aggregate) const
    {
        auto dictionaryId = IdsReader_[valueIndex];
        if (dictionaryId == 0) {
            *value = MakeUnversionedSentinelValue(EValueType::Null, id, aggregate);
        } else {
            TIntegerValueExtractorBase<ValueType, Scan>::SetValue(value, dictionaryId - 1, id, aggregate);
        }
    }

protected:
    using TIdsReader = TCompressedUnsignedVectorReader<ui32, Scan>;
    TIdsReader IdsReader_;

    using TIntegerValueExtractorBase<ValueType, Scan>::ValueReader_;
    using typename TIntegerValueExtractorBase<ValueType, Scan>::TValueReader;

    size_t InitDictionaryReader(const char* ptr)
    {
        const char* begin = ptr;

        ValueReader_ = TValueReader(reinterpret_cast<const ui64*>(ptr));
        ptr += ValueReader_.GetByteSize();

        IdsReader_ = TIdsReader(reinterpret_cast<const ui64*>(ptr));
        ptr += IdsReader_.GetByteSize();

        return ptr - begin;
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
        ptr += TDenseVersionedValueExtractorBase::InitDenseReader(ptr);
        ptr += TDirectIntegerValueExtractorBase<ValueType, true>::InitDirectReader(ptr);
        YCHECK(ptr == data.End());
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
        ptr += TDenseVersionedValueExtractorBase::InitDenseReader(ptr);
        ptr += TDictionaryIntegerValueExtractorBase<ValueType, true>::InitDictionaryReader(ptr);
        YCHECK(ptr == data.End());
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
        : TSparseVersionedValueExtractorBase(meta, aggregate)
        , TDirectIntegerValueExtractorBase<ValueType, true>(meta)
    {
        const char* ptr = data.Begin();
        ptr += TSparseVersionedValueExtractorBase::InitSparseReader(ptr);
        ptr += TDirectIntegerValueExtractorBase<ValueType, true>::InitDirectReader(ptr);
        YCHECK(ptr == data.End());
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
        : TSparseVersionedValueExtractorBase(meta, aggregate)
        , TDictionaryIntegerValueExtractorBase<ValueType, true>(meta)
    {
        const char* ptr = data.Begin();
        ptr += TSparseVersionedValueExtractorBase::InitSparseReader(ptr);
        ptr += TDictionaryIntegerValueExtractorBase<ValueType, true>::InitDictionaryReader(ptr);
        YCHECK(ptr == data.End());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TVersionedIntegerColumnReader
    : public TVersionedColumnReaderBase
{
public:
    TVersionedIntegerColumnReader(const TColumnMeta& columnMeta, int columnId, bool aggregate)
        : TVersionedColumnReaderBase(columnMeta, columnId, aggregate)
    { }

private:
    virtual std::unique_ptr<IVersionedSegmentReader> CreateSegmentReader(int segmentIndex) override
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
                Y_UNREACHABLE();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IVersionedColumnReader> CreateVersionedInt64ColumnReader(
    const TColumnMeta& columnMeta,
    int columnId,
    bool aggregate)
{
    return std::make_unique<TVersionedIntegerColumnReader<EValueType::Int64>>(
        columnMeta,
        columnId,
        aggregate);
}

std::unique_ptr<IVersionedColumnReader> CreateVersionedUint64ColumnReader(
    const TColumnMeta& columnMeta,
    int columnId,
    bool aggregate)
{
    return std::make_unique<TVersionedIntegerColumnReader<EValueType::Uint64>>(
        columnMeta,
        columnId,
        aggregate);
}

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan>
class TDirectDenseUnversionedIntegerValueExtractor
    : public TDirectIntegerValueExtractorBase<ValueType, Scan>
{
public:
    TDirectDenseUnversionedIntegerValueExtractor(TRef data, const TSegmentMeta& meta)
        : TDirectIntegerValueExtractorBase<ValueType, Scan>(meta)
    {
        InitDirectReader(data.Begin());
        YCHECK(ValueReader_.GetSize() == meta.row_count());
    }

private:
    using TDirectIntegerValueExtractorBase<ValueType, Scan>::ValueReader_;
    using TDirectIntegerValueExtractorBase<ValueType, Scan>::InitDirectReader;
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan>
class TDictionaryDenseUnversionedIntegerValueExtractor
    : public TDictionaryIntegerValueExtractorBase<ValueType, Scan>
{
public:
    TDictionaryDenseUnversionedIntegerValueExtractor(TRef data, const TSegmentMeta& meta)
        : TDictionaryIntegerValueExtractorBase<ValueType, Scan>(meta)
    {
        const char* ptr = data.Begin();
        ptr += TDictionaryIntegerValueExtractorBase<ValueType, Scan>::InitDictionaryReader(ptr);
        YCHECK(ptr == data.End());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan>
class TDirectRleUnversionedIntegerValueExtractor
    : public TDirectIntegerValueExtractorBase<ValueType, Scan>
    , public TRleValueExtractorBase<Scan>
{
public:
    TDirectRleUnversionedIntegerValueExtractor(TRef data, const TSegmentMeta& meta)
        : TDirectIntegerValueExtractorBase<ValueType, Scan>(meta)
    {
        const char* ptr = data.Begin();
        ptr += TDirectIntegerValueExtractorBase<ValueType, Scan>::InitDirectReader(ptr);

        RowIndexReader_ = TRowIndexReader(reinterpret_cast<const ui64*>(ptr));
        ptr += RowIndexReader_.GetByteSize();

        YCHECK(ptr == data.End());
    }

private:
    using typename TRleValueExtractorBase<Scan>::TRowIndexReader;
    using TRleValueExtractorBase<Scan>::RowIndexReader_;
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType, bool Scan>
class TDictionaryRleUnversionedIntegerValueExtractor
    : public TDictionaryIntegerValueExtractorBase<ValueType, Scan>
    , public TRleValueExtractorBase<Scan>
{
public:
    TDictionaryRleUnversionedIntegerValueExtractor(TRef data, const TSegmentMeta& meta)
        : TDictionaryIntegerValueExtractorBase<ValueType, Scan>(meta)
    {
        const char* ptr = data.Begin();
        ptr += TDictionaryIntegerValueExtractorBase<ValueType, Scan>::InitDictionaryReader(ptr);

        RowIndexReader_ = TRowIndexReader(reinterpret_cast<const ui64*>(ptr));
        ptr += RowIndexReader_.GetByteSize();

        YCHECK(ptr == data.End());
    }

private:
    using typename TRleValueExtractorBase<Scan>::TRowIndexReader;
    using TRleValueExtractorBase<Scan>::RowIndexReader_;
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
class TUnversionedIntegerColumnReader
    : public TUnversionedColumnReaderBase
{
public:
    TUnversionedIntegerColumnReader(const TColumnMeta& columnMeta, int columnIndex, int columnId)
        : TUnversionedColumnReaderBase(columnMeta, columnIndex, columnId)
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
        auto segmentType = EUnversionedIntegerSegmentType(meta.type());

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
                Y_UNREACHABLE();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedInt64ColumnReader(
    const TColumnMeta& columnMeta,
    int columnIndex,
    int columnId)
{
    return std::make_unique<TUnversionedIntegerColumnReader<EValueType::Int64>>(
        columnMeta,
        columnIndex,
        columnId);
}

std::unique_ptr<IUnversionedColumnReader> CreateUnversionedUint64ColumnReader(
    const TColumnMeta& columnMeta,
    int columnIndex,
    int columnId)
{
    return std::make_unique<TUnversionedIntegerColumnReader<EValueType::Uint64>>(
        columnMeta,
        columnIndex,
        columnId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
