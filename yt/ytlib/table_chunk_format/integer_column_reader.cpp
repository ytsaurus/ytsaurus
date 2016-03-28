#include "integer_column_reader.h"

#include "compressed_integer_vector.h"
#include "column_reader_detail.h"
#include "private.h"

namespace NYT {
namespace NTableChunkFormat {

using namespace NProto;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

template <EValueType ValueType>
void SetIntegerValue(TUnversionedValue* value, ui64 data);

template <>
void SetIntegerValue<EValueType::Int64>(TUnversionedValue* value, ui64 data)
{
    value->Type = EValueType::Int64;
    value->Data.Int64 = ZigZagDecode64(data);
}

template <>
void SetIntegerValue<EValueType::Uint64>(TUnversionedValue* value, ui64 data)
{
    value->Type = EValueType::Uint64;
    value->Data.Uint64 = data;
}

////////////////////////////////////////////////////////////////////////////////

template<EValueType ValueType>
class TIntegerValueExtractorBase
{
protected:
    TCompressedUnsignedVectorReader<ui64> ValueReader_;
    const TIntegerSegmentMeta& Meta_;

    TIntegerValueExtractorBase(const TSegmentMeta& meta)
        : Meta_(meta.GetExtension(TIntegerSegmentMeta::integer_segment_meta))
    { }

    void SetValue(TUnversionedValue* value, i64 valueIndex) const
    {
        SetIntegerValue<ValueType>(value, Meta_.min_value() + ValueReader_[valueIndex]);
    }
};

////////////////////////////////////////////////////////////////////////////////

template<EValueType ValueType>
class TDirectIntegerValueExtractorBase
    : public TIntegerValueExtractorBase<ValueType>
{
public:
    using TIntegerValueExtractorBase<ValueType>::TIntegerValueExtractorBase;

    void ExtractValue(TUnversionedValue* value, i64 valueIndex) const
    {
        if (NullBitmap_[valueIndex]) {
            value->Type = EValueType::Null;
        } else {
            TIntegerValueExtractorBase<ValueType>::SetValue(value, valueIndex);
        }
    }

protected:
    TReadOnlyBitmap<ui64> NullBitmap_;

    using TIntegerValueExtractorBase<ValueType>::ValueReader_;

    size_t InitDirectReader(const char* ptr)
    {
        const char* begin = ptr;

        ValueReader_ = TCompressedUnsignedVectorReader<ui64>(reinterpret_cast<const ui64*>(ptr));
        ptr += ValueReader_.GetByteSize();

        NullBitmap_ = TReadOnlyBitmap<ui64>(
            reinterpret_cast<const ui64*>(ptr),
            ValueReader_.GetSize());
        ptr += NullBitmap_.GetByteSize();

        return ptr - begin;
    }
};

////////////////////////////////////////////////////////////////////////////////

template<EValueType ValueType>
class TDictionaryIntegerValueExtractorBase
    : public TIntegerValueExtractorBase<ValueType>
{
public:
    using TIntegerValueExtractorBase<ValueType>::TIntegerValueExtractorBase;

    void ExtractValue(TUnversionedValue* value, i64 valueIndex) const
    {
        auto id = IdsReader_[valueIndex];
        if (id == 0) {
            value->Type = EValueType::Null;
        } else {
            TIntegerValueExtractorBase<ValueType>::SetValue(value, id - 1);
        }
    }

protected:
    TCompressedUnsignedVectorReader<ui32> IdsReader_;

    using TIntegerValueExtractorBase<ValueType>::ValueReader_;

    size_t InitDictionaryReader(const char* ptr)
    {
        const char* begin = ptr;

        ValueReader_ = TCompressedUnsignedVectorReader<ui64>(reinterpret_cast<const ui64*>(ptr));
        ptr += ValueReader_.GetByteSize();

        IdsReader_ = TCompressedUnsignedVectorReader<ui32>(reinterpret_cast<const ui64*>(ptr));
        ptr += IdsReader_.GetByteSize();

        return ptr - begin;
    }
};

////////////////////////////////////////////////////////////////////////////////

template<EValueType ValueType>
class TDirectDenseVersionedIntegerValueExtractor
    : public TDenseVersionedValueExtractorBase
    , public TDirectIntegerValueExtractorBase<ValueType>
{
public:
    TDirectDenseVersionedIntegerValueExtractor(
        TRef data,
        const TSegmentMeta& meta,
        bool aggregate)
        : TDenseVersionedValueExtractorBase(meta, aggregate)
        , TDirectIntegerValueExtractorBase<ValueType>(meta)
    {
        const char* ptr = data.Begin();
        ptr += TDenseVersionedValueExtractorBase::InitDenseReader(ptr);
        ptr += TDirectIntegerValueExtractorBase<ValueType>::InitDirectReader(ptr);
        YCHECK(ptr == data.End());
    }
};

////////////////////////////////////////////////////////////////////////////////

template<EValueType ValueType>
class TDictionaryDenseVersionedIntegerValueExtractor
    : public TDenseVersionedValueExtractorBase
    , public TDictionaryIntegerValueExtractorBase<ValueType>
{
public:
    TDictionaryDenseVersionedIntegerValueExtractor(
        TRef data,
        const TSegmentMeta& meta,
        bool aggregate)
        : TDenseVersionedValueExtractorBase(meta, aggregate)
        , TDictionaryIntegerValueExtractorBase<ValueType>(meta)
    {
        const char* ptr = data.Begin();
        ptr += TDenseVersionedValueExtractorBase::InitDenseReader(ptr);
        ptr += TDictionaryIntegerValueExtractorBase<ValueType>::InitDictionaryReader(ptr);
        YCHECK(ptr == data.End());
    }
};

////////////////////////////////////////////////////////////////////////////////

template<EValueType ValueType>
class TDirectSparseVersionedIntegerValueExtractor
    : public TSparseVersionedValueExtractorBase
    , public TDirectIntegerValueExtractorBase<ValueType>
{
public:
    TDirectSparseVersionedIntegerValueExtractor(
        TRef data,
        const TSegmentMeta& meta,
        bool aggregate)
        : TSparseVersionedValueExtractorBase(meta, aggregate)
        , TDirectIntegerValueExtractorBase<ValueType>(meta)
    {
        const char* ptr = data.Begin();
        ptr += TSparseVersionedValueExtractorBase::InitSparseReader(ptr);
        ptr += TDirectIntegerValueExtractorBase<ValueType>::InitDirectReader(ptr);
        YCHECK(ptr == data.End());
    }
};

////////////////////////////////////////////////////////////////////////////////

template<EValueType ValueType>
class TDictionarySparseVersionedIntegerValueExtractor
    : public TSparseVersionedValueExtractorBase
    , public TDictionaryIntegerValueExtractorBase<ValueType>
{
public:
    TDictionarySparseVersionedIntegerValueExtractor(
        TRef data,
        const TSegmentMeta& meta,
        bool aggregate)
        : TSparseVersionedValueExtractorBase(meta, aggregate)
        , TDictionaryIntegerValueExtractorBase<ValueType>(meta)
    {
        const char* ptr = data.Begin();
        ptr += TSparseVersionedValueExtractorBase::InitSparseReader(ptr);
        ptr += TDictionaryIntegerValueExtractorBase<ValueType>::InitDictionaryReader(ptr);
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
                YUNREACHABLE();
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

template<EValueType ValueType>
class TDirectDenseUnversionedIntegerValueExtractor
    : public TDirectIntegerValueExtractorBase<ValueType>
{
public:
    TDirectDenseUnversionedIntegerValueExtractor(TRef data, const TSegmentMeta& meta)
        : TDirectIntegerValueExtractorBase<ValueType>(meta)
    {
        TDirectIntegerValueExtractorBase<ValueType>::InitDirectReader(data.Begin());
        YCHECK(ValueReader_.GetSize() == meta.row_count());
    }

private:
    using TDirectIntegerValueExtractorBase<ValueType>::ValueReader_;
};

////////////////////////////////////////////////////////////////////////////////

template<EValueType ValueType>
class TDictionaryDenseUnversionedIntegerValueExtractor
    : public TDictionaryIntegerValueExtractorBase<ValueType>
{
public:
    TDictionaryDenseUnversionedIntegerValueExtractor(TRef data, const TSegmentMeta& meta)
        : TDictionaryIntegerValueExtractorBase<ValueType>(meta)
    {
        const char* ptr = data.Begin();
        ptr += TDictionaryIntegerValueExtractorBase<ValueType>::InitDictionaryReader(ptr);
        YCHECK(ptr == data.End());
    }
};

////////////////////////////////////////////////////////////////////////////////

template<EValueType ValueType>
class TDirectRleUnversionedIntegerValueExtractor
    : public TDirectIntegerValueExtractorBase<ValueType>
    , public TRleValueExtractorBase
{
public:
    TDirectRleUnversionedIntegerValueExtractor(TRef data, const TSegmentMeta& meta)
        : TDirectIntegerValueExtractorBase<ValueType>(meta)
    {
        const char* ptr = data.Begin();
        ptr += TDirectIntegerValueExtractorBase<ValueType>::InitDirectReader(ptr);

        RowIndexReader_ = TCompressedUnsignedVectorReader<ui64>(reinterpret_cast<const ui64*>(ptr));
        ptr += RowIndexReader_.GetByteSize();

        YCHECK(ptr == data.End());
    }
};

////////////////////////////////////////////////////////////////////////////////

template<EValueType ValueType>
class TDictionaryRleUnversionedIntegerValueExtractor
    : public TDictionaryIntegerValueExtractorBase<ValueType>
    , public TRleValueExtractorBase
{
public:
    TDictionaryRleUnversionedIntegerValueExtractor(TRef data, const TSegmentMeta& meta)
        : TDictionaryIntegerValueExtractorBase<ValueType>(meta)
    {
        const char* ptr = data.Begin();

        ptr += TDictionaryIntegerValueExtractorBase<ValueType>::InitDictionaryReader(ptr);

        RowIndexReader_ = TCompressedUnsignedVectorReader<ui64>(reinterpret_cast<const ui64*>(ptr));
        ptr += RowIndexReader_.GetByteSize();
    }
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
    virtual std::unique_ptr<IUnversionedSegmentReader> CreateSegmentReader(int segmentIndex) override
    {
        typedef TDenseUnversionedSegmentReader<
            ValueType,
            TDirectDenseUnversionedIntegerValueExtractor<ValueType>> TDirectDenseReader;

        typedef TDenseUnversionedSegmentReader<
            ValueType,
            TDictionaryDenseUnversionedIntegerValueExtractor<ValueType>> TDictionaryDenseReader;

        typedef TRleUnversionedSegmentReader<
            ValueType,
            TDirectRleUnversionedIntegerValueExtractor<ValueType>> TDirectRleReader;

        typedef TRleUnversionedSegmentReader<
            ValueType,
            TDictionaryRleUnversionedIntegerValueExtractor<ValueType>> TDictionaryRleReader;

        const auto& meta = ColumnMeta_.segments(segmentIndex);
        auto segmentType = EUnversionedIntegerSegmentType(meta.type());

        switch (segmentType) {
            case EUnversionedIntegerSegmentType::DirectDense:
                return DoCreateSegmentReader<TDirectDenseReader>(meta);

            case EUnversionedIntegerSegmentType::DictionaryDense:
                return DoCreateSegmentReader<TDictionaryDenseReader>(meta);

            case EUnversionedIntegerSegmentType::DirectRle:
                return DoCreateSegmentReader<TDirectRleReader>(meta);

            case EUnversionedIntegerSegmentType::DictionaryRle:
                return DoCreateSegmentReader<TDictionaryRleReader>(meta);

            default:
                YUNREACHABLE();
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

} // namespace NTableChunkFormat
} // namespace NYT
