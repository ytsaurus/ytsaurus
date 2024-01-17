#include "rowset_builder.h"
#include "read_span_refiner.h"
#include "dispatch_by_type.h"
#include "reader_statistics.h"
#include "column_block_manager.h"

#include <yt/yt/ytlib/table_client/key_filter.h>

namespace NYT::NNewTableClient {

using NTableClient::NullTimestamp;

using NProfiling::TCpuDurationIncrementingGuard;

////////////////////////////////////////////////////////////////////////////////

struct TValueOutput
{
    TVersionedValue* Ptr;
    const TTimestamp* Timestamps;
    TIdRange IdRange;
};

////////////////////////////////////////////////////////////////////////////////

// TODO(lukyan): Replace data weight with row buffer allocated memory count.
// Data weight does not match actual memory read/write footprint.
// Types of memory are not counted: value types, ids, pointer to string data, length.

template <EValueType Type>
constexpr ui64 GetFixedDataWeightPart(ui64 count)
{
    return !IsStringLikeType(Type) ? count * GetDataWeight(Type) : 0;
}

template <EValueType Type>
Y_FORCE_INLINE ui64 GetVariableDataWeightPart(TUnversionedValue value, ui64 count = 1)
{
    YT_ASSERT(!IsStringLikeType(Type) || value.Type != EValueType::Null || value.Length == 0);
    return IsStringLikeType(Type) ? value.Length * count : 0;
}

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
bool AreTypedValuesEqual(const TUnversionedValue& lhs, const TUnversionedValue& rhs)
{
    TValue lhsValue;
    NTableClient::GetValue(&lhsValue, lhs);
    TValue rhsValue;
    NTableClient::GetValue(&rhsValue, rhs);
    return lhsValue == rhsValue;
}

template <EValueType Type>
bool AreValuesEqual(const TUnversionedValue& lhs, const TUnversionedValue& rhs)
{
    if (Y_UNLIKELY(lhs.Type != rhs.Type)) {
        return false;
    }

    if (rhs.Type == NTableClient::EValueType::Null) {
        return true;
    }

    if constexpr (Type == EValueType::Int64) {
        return AreTypedValuesEqual<i64>(lhs, rhs);
    } else if constexpr (Type == EValueType::Uint64) {
        return AreTypedValuesEqual<ui64>(lhs, rhs);
    } else if constexpr (Type == EValueType::Double) {
        return AreTypedValuesEqual<double>(lhs, rhs);
    } else if constexpr (Type == EValueType::String) {
        return AreTypedValuesEqual<TStringBuf>(lhs, rhs);
    } else if constexpr (Type == EValueType::Boolean) {
        return AreTypedValuesEqual<bool>(lhs, rhs);
    } else if constexpr (Type == EValueType::Any || Type == EValueType::Composite) {
        return CompareRowValues(lhs, rhs) == 0;
    } else if constexpr (Type == EValueType::Null) {
        // Nulls are always equal
        return true;
    } else {
        // Poor man static_assert(false, ...).
        static_assert(Type == EValueType::Int64, "Unexpected value type");
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TReadItem>
class TKeyColumnBase;

template <class TReadItem, EValueType Type>
class TKeyColumn;

template <>
class TKeyColumnBase<TReadSpan>
    : public TColumnBase
    , public TScanKeyIndexExtractor
{
public:
    using TColumnBase::TColumnBase;

    virtual ~TKeyColumnBase() = default;

#ifdef FULL_UNPACK
    virtual ui32 UpdateSegment(ui32 rowIndex, TTmpBuffers* tmpBuffers, bool newMeta) = 0;
#endif
    virtual ui32 UpdateSegment(ui32 rowOffset, TMutableRange<TReadSpan> spans, TTmpBuffers* tmpBuffers, bool newMeta) = 0;

    virtual ui32 ReadKeys(
        TUnversionedValue** keys,
        TRange<TReadSpan> spans,
        ui32 position,
        ui16 columnId,
        ui64* dataWeight) const = 0;
};

template <EValueType Type>
class TKeyColumn<TReadSpan, Type>
    : public TKeyColumnBase<TReadSpan>
    , public TScanDataExtractor<Type>
{
public:
    using TScanDataExtractor<Type>::Extract;

    explicit TKeyColumn(const TColumnBase* columnBase)
        : TKeyColumnBase<TReadSpan>(columnBase)
    {
        if (TKeyColumnBase<TReadSpan>::IsNull()) {
            // Key column not present in chunk.
            TKeyColumnBase<TReadSpan>::InitNullIndex();
            TScanDataExtractor<Type>::InitNullData();
        }
    }

#ifdef FULL_UNPACK
    ui32 UpdateSegment(ui32 rowIndex, TTmpBuffers* tmpBuffers, bool newMeta) override
    {
        auto meta = SkipToSegment<TKeyMeta<Type>>(rowIndex);
        if (meta) {
            auto* ptr = reinterpret_cast<const ui64*>(GetBlock().begin() + meta->DataOffset);
            DoInitRangesKeySegment(this, meta, ptr, tmpBuffers, newMeta);
        }
        return GetSegmentRowLimit();
    }
#endif

    ui32 UpdateSegment(ui32 rowOffset, TMutableRange<TReadSpan> spans, TTmpBuffers* tmpBuffers, bool newMeta) override
    {
        auto startRowIndex = spans.Front().Lower;

        auto meta = SkipToSegment<TKeyMeta<Type>>(startRowIndex);
        if (meta) {
            auto* ptr = reinterpret_cast<const ui64*>(GetBlock().begin() + meta->DataOffset);
            DoInitRangesKeySegment(this, meta, ptr, rowOffset, spans, tmpBuffers, newMeta);
        }
        return GetSegmentRowLimit();
    }

    ui32 ReadKeys(
        TUnversionedValue** keys,
        TRange<TReadSpan> spans,
        ui32 position,
        ui16 columnId,
        ui64* dataWeight) const override
    {
        for (auto [lower, upper] : spans) {
            position = SkipTo(lower, position);
            position = DoReadKeys(keys, lower, upper, position, columnId, dataWeight);
            keys += upper - lower;
        }

        return position;
    }

private:
    Y_FORCE_INLINE ui32 DoReadKeys(
        TUnversionedValue** keys,
        ui32 rowIndex,
        ui32 rowLimit,
        ui32 position,
        ui16 columnId,
        ui64* dataWeight) const
    {
        YT_ASSERT(rowLimit <= GetSegmentRowLimit());
        YT_ASSERT(position < GetCount());
        YT_ASSERT(rowIndex >= LowerRowBound(position));
        YT_ASSERT(rowIndex < UpperRowBound(position));

        // Keep counter in register.
        ui64 localDataWeight = 0;
        while (rowIndex < rowLimit) {
            TUnversionedValue value{};
            value.Id = GetColumnId();

            YT_ASSERT(position < GetCount());
            Extract(&value, position);

            ui32 nextRowIndex = rowLimit < UpperRowBound(position) ? rowLimit : UpperRowBound(position++);
            YT_ASSERT(rowIndex < nextRowIndex);

            auto keysEnd = keys + nextRowIndex - rowIndex;
            rowIndex = nextRowIndex;

            localDataWeight += GetFixedDataWeightPart<Type>(keysEnd - keys) +
                GetVariableDataWeightPart<Type>(value, keysEnd - keys);

            while (keys < keysEnd) {
                (*keys++)[columnId] = value;
            }
        }

        *dataWeight += localDataWeight;

        return position;
    }
};

template <>
class TKeyColumnBase<ui32>
    : public TColumnBase
    , public TLookupKeyIndexExtractor
{
public:
    using TColumnBase::TColumnBase;

    virtual ~TKeyColumnBase() = default;

    virtual ui32 UpdateSegment(ui32 rowIndex, bool newMeta) = 0;

    virtual ui32 ReadKeys(
        TUnversionedValue** keys,
        TRange<ui32> readIndexes,
        ui32 position,
        ui16 columnId,
        ui64* dataWeight) const = 0;

    // This method compares value according its statically determined type.
    virtual bool HasKeyAtIndex(
        const TUnversionedValue& expected,
        ui32 rowIndex) = 0;
};

template <EValueType Type>
class TKeyColumn<ui32, Type>
    : public TKeyColumnBase<ui32>
    , public TLookupDataExtractor<Type>
{
public:
    using TLookupDataExtractor<Type>::Extract;

    explicit TKeyColumn(const TColumnBase* columnBase)
        : TKeyColumnBase<ui32>(columnBase)
    {
        if (TKeyColumnBase<ui32>::IsNull()) {
            // Key column not present in chunk.
            TKeyColumnBase<ui32>::InitNullIndex();
            TLookupDataExtractor<Type>::InitNullData();
        }
    }

    ui32 UpdateSegment(ui32 rowIndex, bool newMeta) override
    {
        auto meta = SkipToSegment<TKeyMeta<Type>>(rowIndex);
        if (meta) {
            auto data = GetBlock().begin() + meta->DataOffset;
            if (newMeta) {
                DoInitLookupKeySegment</*NewMeta*/ true>(this, meta, reinterpret_cast<const ui64*>(data));
            } else {
                DoInitLookupKeySegment</*NewMeta*/ false>(this, meta, reinterpret_cast<const ui64*>(data));
            }
        }
        return GetSegmentRowLimit();
    }

    ui32 ReadKeys(
        TUnversionedValue** keys,
        TRange<ui32> readIndexes,
        ui32 position,
        ui16 columnId,
        ui64* dataWeight) const override
    {
        // Keep counter in register.
        ui64 localDataWeight = 0;
        for (auto readIndex : readIndexes) {
            position = SkipTo(readIndex, position);

            TUnversionedValue value{};
            value.Id = GetColumnId();

            YT_ASSERT(position < GetCount());
            Extract(&value, position);

            localDataWeight += GetFixedDataWeightPart<Type>(1) +
                GetVariableDataWeightPart<Type>(value);

            (*keys++)[columnId] = value;
        }

        *dataWeight += localDataWeight;

        return position;
    }

    bool HasKeyAtIndex(
        const TUnversionedValue& expected,
        ui32 rowIndex) override
    {
        if (rowIndex >= GetSegmentRowLimit()) {
            auto meta = SkipToSegment<TKeyMeta<Type>>(rowIndex);
            if (meta) {
                auto data = GetBlock().begin() + meta->DataOffset;
                DoInitLookupKeySegment</*NewMeta*/ true>(this, meta, reinterpret_cast<const ui64*>(data));
            }
        }

        ui32 position = SkipTo(rowIndex, 0);
        YT_ASSERT(position < GetCount());

        TUnversionedValue value = {};
        Extract(&value, position);

        return AreValuesEqual<Type>(expected, value);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TReadItem>
class TValueColumnBase;

template <class TReadItem, EValueType Type, bool Aggregate, bool ProduceAll>
class TVersionedValueColumn;

////////////////////////////////////////////////////////////////////////////////

template <EValueType Type, bool Aggregate>
class TVersionedValueReader
    : public TScanVersionExtractor<Aggregate>
    , public TScanDataExtractor<Type>
{
public:
    static constexpr bool Aggregate_ = Aggregate;
    static constexpr EValueType Type_ = Type;

    void Init(const TValueMeta<Type>* meta, const ui64* ptr, TTmpBuffers* tmpBuffers, bool newMeta)
    {
        ptr = TScanVersionExtractor<Aggregate>::InitVersion(meta, ptr, newMeta);
        TScanDataExtractor<Type>::InitData(meta, ptr, tmpBuffers, newMeta);
    }

    void Init(
        const TValueMeta<Type>* meta,
        const ui64* ptr,
        TRange<TReadSpan> spans,
        ui32 batchSize,
        TTmpBuffers* tmpBuffers,
        bool newMeta)
    {
        ptr = TScanVersionExtractor<Aggregate>::InitVersion(meta, ptr, spans, batchSize, newMeta);
        TScanDataExtractor<Type>::InitData(meta, ptr, spans, batchSize, tmpBuffers, meta->ChunkRowCount, newMeta);
    }
};

template <EValueType Type, bool Aggregate>
class TVersionedValueLookuper
    : public TLookupVersionExtractor<Aggregate>
    , public TLookupDataExtractor<Type>
{
public:
    static constexpr bool Aggregate_ = Aggregate;
    static constexpr EValueType Type_ = Type;

    template <bool NewMeta>
    Y_FORCE_INLINE void Init(const TValueMeta<Type>* meta, const ui64* ptr)
    {
        ptr = TLookupVersionExtractor<Aggregate>::template InitVersion<NewMeta>(meta, ptr);
        TLookupDataExtractor<Type>::template InitData<NewMeta>(meta, ptr);
    }

    Y_FORCE_INLINE void Prefetch(ui32 valueIdx) const
    {
        TLookupVersionExtractor<Aggregate>::Prefetch(valueIdx);
        TLookupDataExtractor<Type>::Prefetch(valueIdx);
    }
};

template <class TBase, bool ProduceAll, bool AllCommittedOnly = false>
struct TVersionedValueExtractor
    : public TBase
{
    using TBase::Extract;
    using TBase::ExtractVersion;
    using TBase::AdjustLowerIndex;
    using TBase::AdjustIndex;

    using TBase::Aggregate_;
    using TBase::Type_;

    // Returns data weight.
    Y_FORCE_INLINE ui64 operator() (
        ui32 valueIdx,
        ui32 valueIdxEnd,
        TValueOutput* valueOutput,
        ui16 columnId) const
    {
        if constexpr (!AllCommittedOnly) {
            // Adjust valueIdx and valueIdxEnd according to tsIds.
            auto [lowerId, upperId] = valueOutput->IdRange;
            valueIdx = AdjustLowerIndex(valueIdx, valueIdxEnd, lowerId);
            if constexpr (!ProduceAll) {
                valueIdxEnd = AdjustIndex(valueIdx, valueIdxEnd, upperId);
            }

            // lowerId and upperId can bet not equal but there are no values.
            YT_ASSERT((valueIdx == valueIdxEnd) || (lowerId != upperId));
        }

        auto* valuePtr = valueOutput->Ptr;
        auto* timestamps = valueOutput->Timestamps;

        auto valueCount = ProduceAll || Aggregate_ ? valueIdxEnd - valueIdx : 1;
        auto dataWeight = GetFixedDataWeightPart<Type_>(valueCount) + sizeof(TTimestamp) * valueCount;

        while (valueIdx != valueIdxEnd) {
            *valuePtr = {};
            valuePtr->Id = columnId;
            Extract(valuePtr, valueIdx);
            ExtractVersion(valuePtr, timestamps, valueIdx);
            dataWeight += GetVariableDataWeightPart<Type_>(*valuePtr);

            ++valuePtr;
            ++valueIdx;
            if constexpr (!ProduceAll && !Aggregate_) {
                break;
            }
        }

        valueOutput->Ptr = valuePtr;

        return dataWeight;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <EValueType Type, bool Aggregate, bool Direct>
struct TSpecializedVersionedValueLookuper
    : public TVersionedValueLookuper<Type, Aggregate>
{
    using TBase = TVersionedValueLookuper<Type, Aggregate>;
    Y_FORCE_INLINE void Extract(TUnversionedValue* value, ui32 position) const
    {
        if constexpr (Direct) {
            TBase::ExtractDirect(value, position);
        } else {
            TBase::ExtractDict(value, position);
        }
    }
};

template <bool Dense>
struct TSpecializedMultiValueIndexLookuper
    : public TLookupMultiValueIndexExtractor
{
    Y_FORCE_INLINE ui32 SkipTo(ui32 rowIndex, ui32 position) const
    {
        if constexpr (Dense) {
            return SkipToDense(rowIndex, position);
        } else {
            return SkipToSparse(rowIndex, position);
        }
    }
};

template <class TBase>
struct TSkipperTo
    : public TBase
{
    using TBase::SkipTo;

    Y_FORCE_INLINE ui32 operator() (ui32 rowIndex, ui32 position) const
    {
        return SkipTo(rowIndex, position);
    }
};

template <class TBase>
struct TCountsCollector
    : public TBase
{
    using TBase::SkipTo;

    Y_FORCE_INLINE ui32 operator() (ui32* counts, TRange<ui32> readIndexes, ui32 position) const
    {
        YT_ASSERT(!readIndexes.Empty());
        position = SkipTo(readIndexes.Front(), position);
        ui32 startPosition = position;

        for (auto readIndex : readIndexes) {
            position = SkipTo(readIndex, position);
            ui32 valueIdx = position;
            position = SkipTo(readIndex + 1, position);
            ui32 valueIdxEnd = position;
            *counts++ += valueIdxEnd - valueIdx;
        }

        return startPosition;
    }
};

template <class TObject, class TBase, class... TArgs>
Y_FORCE_INLINE auto CallCastedMixin(TBase* base, TArgs... args)
{
    static_assert(sizeof(TObject) == sizeof(TBase));
    return (*static_cast<TObject*>(base))(std::move(args)...);
}

////////////////////////////////////////////////////////////////////////////////

template <>
class TValueColumnBase<ui32>
    : public TColumnBase
    , public TLookupMultiValueIndexExtractor
{
public:
    TValueColumnBase(const TColumnBase* columnBase, bool aggregate)
        : TColumnBase(columnBase)
        , Aggregate_(aggregate)
    { }

    virtual ~TValueColumnBase() = default;

    virtual ui32 UpdateSegment(ui32 rowIndex, bool newMeta) = 0;

    ui32 CollectCounts(ui32* counts, TRange<ui32> readIndexes, ui32 position)
    {
        return DoCollectCounts_(this, counts, readIndexes, position);
    }

    ui32 ReadValues(
        TValueOutput* valueOutput,
        TRange<ui32> readIndexes,
        ui32 position,
        ui64* dataWeight) const
    {
        return DoReadValues_(this, valueOutput, readIndexes, position, GetColumnId(), dataWeight);
    }

    virtual void InitAndPrefetch(ui32 rowIndex) = 0;
    virtual TReadSpan SkipToValueAndPrefetch(ui32 rowIndex) = 0;
    // Returns data weight
    virtual ui64 ReadValue(TValueOutput* valueOutput, TReadSpan valueSpan) = 0;

    bool IsAggregate() const
    {
        return Aggregate_;
    }

protected:
    using TDoCollectCounts = ui32 (*)(
        const TLookupMultiValueIndexExtractor* base,
        ui32* counts,
        TRange<ui32> readIndexes,
        ui32 position);

    using TDoReadValues = ui32 (*)(
        const TLookupMultiValueIndexExtractor* base,
        TValueOutput* valueOutput,
        TRange<ui32> readIndexes,
        ui32 position,
        ui16 columnId,
        ui64* dataWeight);

    TDoCollectCounts DoCollectCounts_ = nullptr;
    TDoReadValues DoReadValues_ = nullptr;

    // This field allows to check aggregate flag without calling virtual method.
    const bool Aggregate_;
};

template <EValueType Type, bool Aggregate, bool ProduceAll>
class TVersionedValueColumn<ui32, Type, Aggregate, ProduceAll>
    : public TValueColumnBase<ui32>
    , public TVersionedValueLookuper<Type, Aggregate>
{
public:
    using TVersionedValueBase = TVersionedValueLookuper<Type, Aggregate>;

    TVersionedValueColumn(const TColumnBase* columnBase)
        : TValueColumnBase<ui32>(columnBase, Aggregate)
    { }

    ui32 UpdateSegment(ui32 rowIndex, bool newMeta) override
    {
        auto meta = SkipToSegment<TValueMeta<Type>>(rowIndex);
        if (!meta) {
            return GetSegmentRowLimit();
        }

        DoInitialize(meta, newMeta);

        return meta->ChunkRowCount;
    }

    void InitAndPrefetch(ui32 rowIndex) override
    {
        if (rowIndex >= GetSegmentRowLimit()) {
            auto meta = SkipToSegment<TValueMeta<Type>>(rowIndex);
            auto* ptr = reinterpret_cast<const ui64*>(GetBlock().begin() + meta->DataOffset);

            ptr = TValueColumnBase<ui32>::template InitIndex</*NewMeta*/ true>(meta, ptr);
            TVersionedValueBase::template Init</*NewMeta*/ true>(meta, ptr);
        }

        TValueColumnBase<ui32>::Prefetch(rowIndex);
    }

    TReadSpan SkipToValueAndPrefetch(ui32 rowIndex) override
    {
        ui32 valueIdx = TValueColumnBase<ui32>::SkipTo(rowIndex, 0);
        ui32 valueIdxEnd = TValueColumnBase<ui32>::SkipTo(rowIndex + 1, valueIdx);
        if (valueIdx != valueIdxEnd) {
            TVersionedValueBase::Prefetch(valueIdx);
        }
        return {valueIdx, valueIdxEnd};
    }

    ui64 ReadValue(TValueOutput* valueOutput, TReadSpan valueSpan) override
    {
        return CallCastedMixin<
            const TVersionedValueExtractor<TVersionedValueBase, ProduceAll>,
            const TVersionedValueBase>
            (static_cast<const TVersionedValueColumn*>(this), valueSpan.Lower, valueSpan.Upper, valueOutput, GetColumnId());
    }

    template <class TIndexExtractorBase, class TVersionedValueExtractorBase>
    static ui32 DoReadValues(
        const TLookupMultiValueIndexExtractor* base,
        TValueOutput* valueOutput,
        TRange<ui32> rowIndexes,
        ui32 position,
        ui16 columnId,
        ui64* dataWeight)
    {
        // Keep counter in register.
        ui64 localDataWeight = 0;
        for (auto rowIndex : rowIndexes) {
            position = CallCastedMixin<const TSkipperTo<TIndexExtractorBase>, const TLookupMultiValueIndexExtractor>
                (base, rowIndex, position);
            ui32 valueIdx = position;
            position = CallCastedMixin<const TSkipperTo<TIndexExtractorBase>, const TLookupMultiValueIndexExtractor>
                (base, rowIndex + 1, position);
            ui32 valueIdxEnd = position;

            localDataWeight += CallCastedMixin<
                const TVersionedValueExtractor<TVersionedValueExtractorBase, ProduceAll>,
                const TVersionedValueBase>
                (static_cast<const TVersionedValueColumn*>(base), valueIdx, valueIdxEnd, valueOutput, columnId);

            ++valueOutput;
        }
        *dataWeight += localDataWeight;

        return position;
    }

private:
    void DoInitialize(const TValueMeta<Type>* meta, bool newMeta)
    {
        auto* ptr = reinterpret_cast<const ui64*>(GetBlock().begin() + meta->DataOffset);

        if (newMeta) {
            ptr = TValueColumnBase<ui32>::template InitIndex</*NewMeta*/ true>(meta, ptr);
            TVersionedValueBase::template Init</*NewMeta*/ true>(meta, ptr);
        } else {
            ptr = TValueColumnBase<ui32>::template InitIndex</*NewMeta*/ false>(meta, ptr);
            TVersionedValueBase::template Init</*NewMeta*/ false>(meta, ptr);
        }

#ifdef USE_UNSPECIALIZED_SEGMENT_READERS
        DoCollectCounts_ = &CallCastedMixin<
            const TCountsCollector<TLookupMultiValueIndexExtractor>,
            const TLookupMultiValueIndexExtractor,
            ui32*,
            TRange<ui32>,
            ui32>;

        DoReadValues_ = &DoReadValues<TLookupMultiValueIndexExtractor, TVersionedValueBase>;
#else
        if (meta->IsDense()) {
            DoCollectCounts_ = GetCollectCountsRoutine<true>();
            DoReadValues_ = GetReadValuesRoutine<true>(meta);
        } else {
            DoCollectCounts_ = GetCollectCountsRoutine<false>();
            DoReadValues_ = GetReadValuesRoutine<false>(meta);
        }
#endif
    }

    template <bool Dense>
    static TDoCollectCounts GetCollectCountsRoutine()
    {
        using TIndexExtractorBase = TSpecializedMultiValueIndexLookuper<Dense>;

        return &CallCastedMixin<
            const TCountsCollector<TIndexExtractorBase>,
            const TLookupMultiValueIndexExtractor,
            ui32*,
            TRange<ui32>,
            ui32>;
    }

    template <bool Dense>
    static TDoReadValues GetReadValuesRoutine(const TValueMeta<Type>* meta)
    {
        using TIndexExtractorBase = TSpecializedMultiValueIndexLookuper<Dense>;

        if constexpr (Type == EValueType::Double || Type == EValueType::Boolean) {
            return &DoReadValues<TIndexExtractorBase, TVersionedValueBase>;
        } else {
            return meta->Direct
                ? &DoReadValues<TIndexExtractorBase, TSpecializedVersionedValueLookuper<Type, Aggregate, true>>
                : &DoReadValues<TIndexExtractorBase, TSpecializedVersionedValueLookuper<Type, Aggregate, false>>;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <>
class TValueColumnBase<TReadSpan>
    : public TColumnBase
    , public TScanMultiValueIndexExtractor
{
public:
    TValueColumnBase(const TColumnBase* columnBase, bool aggregate)
        : TColumnBase(columnBase)
        , Aggregate_(aggregate)
    { }

    virtual ~TValueColumnBase() = default;

#ifdef FULL_UNPACK
    virtual ui32 UpdateSegment(ui32 rowIndex, TTmpBuffers* tmpBuffers, bool newMeta) = 0;
#endif
    virtual ui32 UpdateSegment(ui32 rowOffset, TMutableRange<TReadSpan> spans, TTmpBuffers* tmpBuffers, bool newMeta) = 0;

    ui32 CollectCounts(ui32* counts, TRange<TReadSpan> spans, ui32 position) const
    {
        YT_ASSERT(!spans.Empty());
        position = SkipTo(spans.Front().Lower, position);
        ui32 startPosition = position;

        for (auto [lower, upper] : spans) {
            position = DoCollectCounts(counts, lower, upper, position);
            counts += upper - lower;
        }

        return startPosition;
    }

    virtual ui32 ReadValues(
        TValueOutput* valueOutput,
        TRange<TReadSpan> spans,
        ui32 position,
        ui64* dataWeight) const = 0;

    bool IsAggregate() const
    {
        return Aggregate_;
    }

private:
    // This field allows to check aggregate flag without calling virtual method.
    const bool Aggregate_;

    ui32 DoCollectCounts(
        ui32* counts,
        ui32 rowIndex,
        ui32 rowLimit,
        ui32 position) const
    {
        YT_ASSERT(rowLimit <= GetSegmentRowLimit());

        position = SkipTo(rowIndex, position);

        // Modified variables are local and can be located in registers.
        auto rowToValue = RowToValue_.GetData() + position;

        ui32 valueIdx = rowToValue->ValueOffset;
        while (rowToValue->RowIndex < rowLimit) {
            YT_ASSERT(rowToValue < RowToValue_.GetData() + IndexCount_);

            ui32 skipCount = rowToValue->RowIndex - rowIndex;
            counts += skipCount;
            rowIndex = rowToValue->RowIndex + 1;

            ++rowToValue;
            // auto valueCount = rowToValue[1].ValueIndex - rowToValue[0].ValueIndex;
            // Use valueIdx from previous iteration.
            auto valueCount = rowToValue->ValueOffset - valueIdx;
            valueIdx = rowToValue->ValueOffset;

            *counts++ += valueCount;
        }

        return rowToValue - RowToValue_.GetData();
    }
};

template <EValueType Type, bool Aggregate, bool ProduceAll>
class TVersionedValueColumn<TReadSpan, Type, Aggregate, ProduceAll>
    : public TValueColumnBase<TReadSpan>
    , public TVersionedValueReader<Type, Aggregate>
{
public:
    using TVersionedValueBase = TVersionedValueReader<Type, Aggregate>;

    using TValueColumnBase<TReadSpan>::SkipTo;

    TVersionedValueColumn(const TColumnBase* columnBase)
        : TValueColumnBase<TReadSpan>(columnBase, Aggregate)
    { }

#ifdef FULL_UNPACK
    ui32 UpdateSegment(ui32 rowIndex, TTmpBuffers* tmpBuffers, bool newMeta) override
    {
        auto meta = SkipToSegment<TValueMeta<Type>>(rowIndex);
        if (meta) {
            auto* ptr = reinterpret_cast<const ui64*>(GetBlock().begin() + meta->DataOffset);
            ptr = TValueColumnBase<TReadSpan>::InitIndex(meta, ptr, tmpBuffers, newMeta);
            TVersionedValueBase::Init(meta, ptr, tmpBuffers, newMeta);
        }

        return GetSegmentRowLimit();
    }
#endif

    ui32 UpdateSegment(ui32 rowOffset, TMutableRange<TReadSpan> spans, TTmpBuffers* tmpBuffers, bool newMeta) override
    {
        auto startRowIndex = spans.Front().Lower;

        auto meta = SkipToSegment<TValueMeta<Type>>(startRowIndex);
        if (meta) {
            auto* ptr = reinterpret_cast<const ui64*>(GetBlock().begin() + meta->DataOffset);

            ptr = TValueColumnBase<TReadSpan>::InitIndex(meta, ptr, rowOffset, spans, tmpBuffers, newMeta);
            ui32 valueCount = TValueColumnBase<TReadSpan>::GetValueCount();
            TVersionedValueBase::Init(meta, ptr, tmpBuffers->DataSpans, valueCount, tmpBuffers, newMeta);
        }

        return GetSegmentRowLimit();
    }

    ui32 ReadValues(
        TValueOutput* valueOutput,
        TRange<TReadSpan> spans,
        ui32 position,
        ui64* dataWeight) const override
    {
        for (auto [lower, upper] : spans) {
            position = SkipTo(lower, position);
            position = DoReadValues(valueOutput, lower, upper, position, dataWeight);
            valueOutput += upper - lower;
        }

        return position;
    }

private:
    Y_FORCE_INLINE ui32 DoReadValues(
        TValueOutput* valueOutput,
        ui32 rowIndex,
        ui32 rowLimit,
        ui32 position,
        ui64* dataWeight) const
    {
        YT_ASSERT(rowLimit <= GetSegmentRowLimit());

        auto rowToValue = RowToValue_.GetData() + position;
        YT_ASSERT(rowIndex <= rowToValue->RowIndex);

        // Keep counter in register.
        ui64 localDataWeight = 0;
        while (rowToValue->RowIndex < rowLimit) {
            YT_ASSERT(rowToValue < RowToValue_.GetData() + IndexCount_);

            ui32 skipCount = rowToValue->RowIndex - rowIndex;
            valueOutput += skipCount;
            rowIndex = rowToValue->RowIndex + 1;

            ui32 valueIdx = rowToValue->ValueOffset;
            ++rowToValue;
            ui32 valueIdxEnd = rowToValue->ValueOffset;
            YT_ASSERT(valueIdx != valueIdxEnd);

            localDataWeight += CallCastedMixin<
                const TVersionedValueExtractor<TVersionedValueBase, ProduceAll>,
                const TVersionedValueBase>
                (this, valueIdx, valueIdxEnd, valueOutput, GetColumnId());

            ++valueOutput;
        }
        *dataWeight += localDataWeight;

        return rowToValue - RowToValue_.GetData();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TReadItem>
struct TTimestampExtractor;

template <>
struct TTimestampExtractor<TReadSpan>
    : public TScanTimestampExtractor
    , public TColumnBase
{
    using TColumnBase::TColumnBase;

    ui32 UpdateSegment(ui32 rowOffset, TMutableRange<TReadSpan> spans, TTmpBuffers* tmpBuffers, bool newMeta)
    {
        auto startRowIndex = spans.Front().Lower;

        auto meta = SkipToSegment<TTimestampMeta>(startRowIndex);
        if (meta) {
            auto data = GetBlock().begin() + meta->DataOffset;
            InitSegment(meta, data, rowOffset, spans, tmpBuffers, newMeta);
        }
        return GetSegmentRowLimit();
    }

    ui32 UpdateSegment(ui32 rowIndex, TTmpBuffers* tmpBuffers, bool newMeta)
    {
        auto meta = SkipToSegment<TTimestampMeta>(rowIndex);
        if (meta) {
            auto data = GetBlock().begin() + meta->DataOffset;
            InitSegment(meta, data, tmpBuffers, newMeta);
        }
        return GetSegmentRowLimit();
    }

};

template <>
struct TTimestampExtractor<ui32>
    : public TLookupTimestampExtractor
    , public TColumnBase
{
    using TColumnBase::TColumnBase;

    ui32 UpdateSegment(ui32 rowIndex, bool newMeta)
    {
        auto meta = SkipToSegment<TTimestampMeta>(rowIndex);
        if (meta) {
            auto data = GetBlock().begin() + meta->DataOffset;

            if (newMeta) {
                InitSegment</*NewMeta*/ true>(meta, data);
            } else {
                InitSegment</*NewMeta*/ false>(meta, data);
            }
        }
        return GetSegmentRowLimit();
    }

};

////////////////////////////////////////////////////////////////////////////////

// TODO(lukyan): Parametrize TRowAllocatorBase with TScanTimestampExtractor or TLookupTimestampExtractor
// instead of TReadItem?
template <class TReadItem>
class TRowAllocatorBase
    : public TTimestampExtractor<TReadItem>
{
public:
    using TTimestampExtractorBase = TTimestampExtractor<TReadItem>;

    using TTimestampExtractorBase::GetWriteTimestamps;
    using TTimestampExtractorBase::GetDeleteTimestamps;

    TRowAllocatorBase(
        const TColumnBase& timestampColumnInfo,
        TTimestamp timestamp,
        bool produceAll)
        : TTimestampExtractorBase(&timestampColumnInfo)
        , Timestamp_(timestamp)
        , ProduceAll_(produceAll)
    { }

    TMutableVersionedRow DoAllocateRow(
        TChunkedMemoryPool* memoryPool,
        TValueOutput* valueOutput,
        ui32 keySize,
        ui32 valueCount,
        ui32 rowIndex) const
    {
        auto writeTimestamps = GetWriteTimestamps(rowIndex, memoryPool);
        auto deleteTimestamps = GetDeleteTimestamps(rowIndex, memoryPool);

#ifndef NDEBUG
            for (int index = 1; index < std::ssize(writeTimestamps); ++index) {
                YT_VERIFY(writeTimestamps[index - 1] > writeTimestamps[index]);
            }

            for (int index = 1; index < std::ssize(deleteTimestamps); ++index) {
                YT_VERIFY(deleteTimestamps[index - 1] > deleteTimestamps[index]);
            }
#endif

        auto [lowerDeleteIt, lowerWriteIt] = GetLowerTimestampsIndexes(
            deleteTimestamps.Begin(),
            deleteTimestamps.End(),
            writeTimestamps.Begin(),
            writeTimestamps.End(),
            Timestamp_);

        // COMPAT(lukyan): Produce really all versions or all versions after last delete.
        if (ProduceAll_) {
            // Produces all versions and all delete timestamps.
            auto timestampIdRange = std::pair(
                lowerWriteIt - writeTimestamps.Begin(),
                writeTimestamps.Size());

            auto row = TMutableVersionedRow::Allocate(
                memoryPool,
                keySize,
                valueCount,
                writeTimestamps.End() - lowerWriteIt,
                deleteTimestamps.End() - lowerDeleteIt);

            std::copy(lowerWriteIt, writeTimestamps.End(), row.BeginWriteTimestamps());
            std::copy(lowerDeleteIt, deleteTimestamps.End(), row.BeginDeleteTimestamps());

            *valueOutput = {row.BeginValues(), writeTimestamps.Begin(), timestampIdRange};
            return row;
        } else {
            // In case of all versions produce only versions after latest (before read timestamp) delete.
            auto deleteTimestamp = lowerDeleteIt != deleteTimestamps.End()
                ? *lowerDeleteIt
                : NullTimestamp;
            auto upperWriteIt = GetUpperWriteIndex(lowerWriteIt, writeTimestamps.End(), deleteTimestamp);

            auto timestampIdRange = std::pair(
                lowerWriteIt - writeTimestamps.Begin(),
                upperWriteIt - writeTimestamps.Begin());

            auto row = TMutableVersionedRow::Allocate(
                memoryPool,
                keySize,
                valueCount,
                upperWriteIt != lowerWriteIt ? 1 : 0,
                deleteTimestamp != NullTimestamp ? 1 : 0);

            if (lowerWriteIt != upperWriteIt) {
                row.WriteTimestamps()[0] = *lowerWriteIt;
            }

            if (deleteTimestamp != NullTimestamp) {
                row.DeleteTimestamps()[0] = deleteTimestamp;
            }

            *valueOutput = {row.BeginValues(), writeTimestamps.Begin(), timestampIdRange};
            return row;
        }
    }

protected:
    const TTimestamp Timestamp_;
    const bool ProduceAll_;

    static std::pair<const TTimestamp*, const TTimestamp*> GetLowerTimestampsIndexes(
        const TTimestamp* deleteBegin,
        const TTimestamp* deleteEnd,
        const TTimestamp* writeBegin,
        const TTimestamp* writeEnd,
        TTimestamp readTimestamp)
    {
        // Timestamps inside row are sorted in reverse order.
        // Get delete timestamp.
        auto lowerDeleteIt = BinarySearch(deleteBegin, deleteEnd, [&] (auto it) {
            return *it > readTimestamp;
        });

        // Get write timestamp.
        auto lowerWriteIt = BinarySearch(writeBegin, writeEnd, [&] (auto it) {
            return *it > readTimestamp;
        });

        return std::pair(lowerDeleteIt, lowerWriteIt);
    }

    static const TTimestamp* GetUpperWriteIndex(
        const TTimestamp* writeBegin,
        const TTimestamp* writeEnd,
        TTimestamp deleteTimestamp)
    {
        if (writeBegin == writeEnd || *writeBegin <= deleteTimestamp) {
            return writeBegin;
        }

        // UpperWriteId is used for aggregates.
        const TTimestamp* upperWriteIt;
        if (deleteTimestamp == NullTimestamp) {
            upperWriteIt = writeEnd;
        } else {
            // Keep only binary search?
            upperWriteIt = BinarySearch(writeBegin + 1, writeEnd, [&] (auto it) {
                return *it > deleteTimestamp;
            });
        }

        return upperWriteIt;
    }

};

template <class TFunctor>
void ForEachRowIndex(TRange<TReadSpan> readList, TFunctor functor)
{
    for (auto [lower, upper] : readList) {
        auto batchSize = upper - lower;
        YT_VERIFY(batchSize);

        for (ui32 index = 0; index < batchSize; ++index) {
            functor(lower + index);
        }
    }
}

template <class TFunctor>
void ForEachRowIndex(TRange<ui32> readList, TFunctor functor)
{
    for (auto index : readList) {
        functor(index);
    }
}

ui32 GetRowCount(TRange<TReadSpan> readList)
{
    ui32 batchSize = 0;
    for (auto [lower, upper] : readList) {
        YT_VERIFY(lower != upper);
        batchSize += upper - lower;
    }
    return batchSize;
}

ui32 GetRowCount(TRange<ui32> readList)
{
    return readList.size();
}

template <class T>
T* Allocate(TChunkedMemoryPool* pool, size_t size)
{
    return reinterpret_cast<T*>(pool->AllocateAligned(sizeof(T) * size));
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class... TArgs>
T* ConstructObjectInplace(char** memory, TArgs&&... args)
{
    auto result = new (*memory) T(std::forward<TArgs>(args)...);
    *memory += sizeof(T);
    return result;
}

struct TDestructorCaller
{
    template <class T>
    void operator() (T* ptr)
    {
        ptr->~T();
    }
};

DEFINE_ENUM(ECreateKeyReadersMode,
    (None)
    (ByIndexes)
    (All)
);

////////////////////////////////////////////////////////////////////////////////

template <class TReadItem>
class TRowsetBuilder
    : public IRowsetBuilder
    , public TRowAllocatorBase<TReadItem>
{
public:
    using TBase = TRowAllocatorBase<TReadItem>;
    using TBase::ProduceAll_;
    using TBase::Timestamp_;

    using TKeyColumnHolder = std::unique_ptr<TKeyColumnBase<TReadItem>, TDestructorCaller>;
    using TValueColumnHolder = std::unique_ptr<TValueColumnBase<TReadItem>, TDestructorCaller>;

    template <EValueType Type>
    struct TCreateKeyColumn
    {
        static TKeyColumnHolder Do(
            char** memoryArea,
            const TColumnBase* columnBase)
        {
            return TKeyColumnHolder{ConstructObjectInplace<TKeyColumn<TReadItem, Type>>(
                memoryArea,
                columnBase)};
        }
    };

    template <EValueType Type>
    struct TCreateVersionedValueColumn
    {
        template <bool Aggregate>
        static TValueColumnHolder DoInner(
            char** memoryArea,
            const TColumnBase* columnBase,
            bool produceAll)
        {
            if (produceAll) {
                return TValueColumnHolder{ConstructObjectInplace<TVersionedValueColumn<TReadItem, Type, Aggregate, true>>(
                    memoryArea,
                    columnBase)};
            } else {
                return TValueColumnHolder{ConstructObjectInplace<TVersionedValueColumn<TReadItem, Type, Aggregate, false>>(
                    memoryArea,
                    columnBase)};
            }
        }

        static TValueColumnHolder Do(
            char** memoryArea,
            const TColumnBase* columnBase,
            bool aggregate,
            bool produceAll)
        {
            if (aggregate) {
                return DoInner<true>(memoryArea, columnBase, produceAll);
            } else {
                return DoInner<false>(memoryArea, columnBase, produceAll);
            }
        }
    };

    template <EValueType Type>
    static constexpr size_t GetValueColumnMaxSize()
    {
        return std::max({
            sizeof(TVersionedValueColumn<TReadItem, Type, false, false>),
            sizeof(TVersionedValueColumn<TReadItem, Type, false, true>),
            sizeof(TVersionedValueColumn<TReadItem, Type, true, false>),
            sizeof(TVersionedValueColumn<TReadItem, Type, true, true>),
        });
    }

    template <EValueType... Types>
    static constexpr size_t GetKeyColumnsMaxSize()
    {
        return std::max({sizeof(TKeyColumn<TReadItem, Types>)...});
    }

    template <EValueType... Types>
    static constexpr size_t GetValueColumnsMaxSize()
    {
        return std::max({GetValueColumnMaxSize<Types>()...});
    }

    static constexpr size_t KeyColumnMaxSize = GetKeyColumnsMaxSize<
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double,
        EValueType::Boolean,
        EValueType::String,
        EValueType::Any,
        EValueType::Composite
    >();

    static constexpr size_t ValueColumnMaxSize = GetValueColumnsMaxSize<
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double,
        EValueType::Boolean,
        EValueType::String,
        EValueType::Any,
        EValueType::Composite
    >();

    explicit TRowsetBuilder(TRowsetBuilderParams params, ECreateKeyReadersMode createKeyReadersMode)
        : TBase(
            // Init timestamp column.
            params.ColumnInfos.Back(),
            params.Timestamp,
            params.ProduceAll)
        , NewMeta_(params.NewMeta)
    {
        int keyReadersCount = 0;
        if (createKeyReadersMode == ECreateKeyReadersMode::All) {
            keyReadersCount = std::ssize(params.KeyTypes);
        } else if (createKeyReadersMode == ECreateKeyReadersMode::ByIndexes) {
            keyReadersCount = std::ssize(params.KeyColumnIndexes);
        }

        auto columnReadersMemorySize =
            keyReadersCount * KeyColumnMaxSize +
            std::ssize(params.ValueSchema) * ValueColumnMaxSize;
        auto positionsMemorySize = sizeof(ui32) * (keyReadersCount + params.ValueSchema.size());

        ColumnReadersMemoryHolder_ = std::make_unique<char[]>(
            columnReadersMemorySize +
            positionsMemorySize);

        auto* memoryArea = ColumnReadersMemoryHolder_.get();
        const auto* columnInfoIt = params.ColumnInfos.begin();

        KeyColumns_.reserve(keyReadersCount);

        if (createKeyReadersMode == ECreateKeyReadersMode::All) {
            for (auto type : params.KeyTypes) {
                KeyColumns_.push_back(DispatchByDataType<TCreateKeyColumn>(type, &memoryArea, columnInfoIt++));
            }
        } else if (createKeyReadersMode == ECreateKeyReadersMode::ByIndexes) {
            int extraKeyColumnIndex = params.ReadItemWidth;
            for (auto keyColumnIndex : params.KeyColumnIndexes) {
                auto type = params.KeyTypes[keyColumnIndex];
                const TColumnBase* columnBase = nullptr;
                if (keyColumnIndex < params.ReadItemWidth) {
                    columnBase = columnInfoIt + keyColumnIndex;
                } else {
                    columnBase = columnInfoIt + extraKeyColumnIndex;
                    ++extraKeyColumnIndex;
                }
                KeyColumns_.push_back(DispatchByDataType<TCreateKeyColumn>(type, &memoryArea, columnBase));
            }

            columnInfoIt += extraKeyColumnIndex;
        } else {
            columnInfoIt += params.ReadItemWidth;
        }

        ValueColumns_.reserve(std::ssize(params.ValueSchema));
        for (auto [columnId, type, aggregate] : params.ValueSchema) {
            ValueColumns_.push_back(DispatchByDataType<TCreateVersionedValueColumn>(
                type,
                &memoryArea,
                columnInfoIt++,
                aggregate,
                params.ProduceAll));
        }

        YT_VERIFY(memoryArea <= ColumnReadersMemoryHolder_.get() + columnReadersMemorySize);

        Positions_ = reinterpret_cast<ui32*>(memoryArea);
        memset(Positions_, 0, positionsMemorySize);
    }

    ui16 GetKeyColumnCount() const
    {
        return KeyColumns_.size();
    }

    ui32 ReadRowsWithoutKeys(
        TMutableVersionedRow* rows,
        TRange<TReadItem> readList,
        ui64* dataWeight,
        int targetKeyColumnCount,
        TReaderStatistics* readerStatistics)
    {
        ++readerStatistics->DoReadCallCount;
        if (readList.Empty()) {
            return 0;
        }
        ui32 batchSize = GetRowCount(readList);

        ValueCounts_.Resize(batchSize);
        auto valueCounts = ValueCounts_.GetData();
        std::fill_n(valueCounts, batchSize, 0);

        {
            TCpuDurationIncrementingGuard timingGuard(&readerStatistics->CollectCountsTime);

            ui16 fixedValueCountColumns = 0;
            ui16 columnId = GetKeyColumnCount();
            for (const auto& column : ValueColumns_) {
                if (ProduceAll_ || column->IsAggregate()) {
                    Positions_[columnId] = column->CollectCounts(valueCounts, readList, Positions_[columnId]);
                } else {
                    ++fixedValueCountColumns;
                }
                ++columnId;
            }

            for (ui32 index = 0; index < batchSize; ++index) {
                valueCounts[index] += fixedValueCountColumns;
            }
        }

        ValueOutput_.Resize(batchSize);
        auto valueOutput = ValueOutput_.GetData();

        {
            TCpuDurationIncrementingGuard timingGuard(&readerStatistics->AllocateRowsTime);
            AllocateRows(rows, valueOutput, targetKeyColumnCount, valueCounts, readList);
        }

        {
            TCpuDurationIncrementingGuard timingGuard(&readerStatistics->DoReadValuesTime);
            ReadValues(rows, valueOutput, readList, batchSize, dataWeight);
        }

        return batchSize;
    }

    TChunkedMemoryPool* GetPool() override
    {
        return &MemoryPool_;
    }

    void ClearBuffer() override
    {
        MemoryPool_.Clear();
    }

protected:
    std::unique_ptr<char[]> ColumnReadersMemoryHolder_;
    std::vector<TKeyColumnHolder> KeyColumns_;
    std::vector<TValueColumnHolder> ValueColumns_;
    const bool NewMeta_;

    TChunkedMemoryPool MemoryPool_;

    // Positions in segments are kept separately to minimize write memory footprint.
    // Column readers are immutable during read.
    ui32* Positions_;

    TMemoryHolder<ui32> ValueCounts_;
    TMemoryHolder<TValueOutput> ValueOutput_;

    void AllocateRows(
        TMutableVersionedRow* rows,
        TValueOutput* valueOutput,
        ui32 keySize,
        const ui32* valueCounts,
        TRange<TReadItem> readList)
    {
        ui32 index = 0;

        ForEachRowIndex(readList, [&] (ui32 rowIndex) {
            rows[index] = TBase::DoAllocateRow(
                &MemoryPool_,
                valueOutput + index,
                keySize,
                valueCounts[index],
                rowIndex);
            ++index;
        });
    }

    void ReadKeys(
        TMutableVersionedRow* rows,
        TRange<TReadItem> readList,
        ui32 batchSize,
        ui64* dataWeight)
    {
        auto rowKeys = Allocate<TUnversionedValue*>(GetPool(), batchSize);
        for (ui32 index = 0; index < batchSize; ++index) {
            rowKeys[index] = rows[index].BeginKeys();
        }

        ui16 columnId = 0;
        for (const auto& column : KeyColumns_) {
            Positions_[columnId] = column->ReadKeys(rowKeys, readList, Positions_[columnId], columnId, dataWeight);
            ++columnId;
        }
    }

    void ReadValues(
        TMutableVersionedRow* rows,
        TValueOutput* valueOutput,
        TRange<TReadItem> readList,
        ui32 batchSize,
        ui64* dataWeight)
    {
        ui16 columnId = GetKeyColumnCount();
        for (const auto& column : ValueColumns_) {
            Positions_[columnId] = column->ReadValues(valueOutput, readList, Positions_[columnId], dataWeight);
            ++columnId;
        }

        auto rowsEnd = rows + batchSize;
        while (rows < rowsEnd) {
            *dataWeight += rows->GetWriteTimestampCount() * sizeof(TTimestamp);
            *dataWeight += rows->GetDeleteTimestampCount() * sizeof(TTimestamp);

            rows->SetValueCount(valueOutput->Ptr - rows->BeginValues());

            ++valueOutput;
            ++rows;
        }
    }

    // Keys are present in chunk but no values (i.e. row is deleted).
    void SetRowsWithoutValuesToSentinels(TMutableVersionedRow* rows, ui32 batchSize)
    {
        auto rowsEnd = rows + batchSize;
        while (rows < rowsEnd) {
            if (rows->GetDeleteTimestampCount() == 0 &&
                rows->GetWriteTimestampCount() == 0 &&
                Timestamp_ != NTableClient::AllCommittedTimestamp)
            {
                // Key is present in chunk but no values corresponding to requested timestamp.
                *rows = {};
            }
            ++rows;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct IColumnRefiner
{
    virtual ~IColumnRefiner() = default;

    virtual void Refine(
        TBoundsIterator<T>* keys,
        const std::vector<TSpanMatching>& matchings,
        std::vector<TSpanMatching>* nextMatchings) = 0;
};

template <class T, EValueType Type>
struct TColumnRefiner
    : public IColumnRefiner<T>
    , public TColumnIterator<Type>
    , public TColumnBase
{
    using TBase = TColumnIterator<Type>;

    using TColumnBase::TColumnBase;

    void Refine(
        TBoundsIterator<T>* keys,
        const std::vector<TSpanMatching>& matchings,
        std::vector<TSpanMatching>* nextMatchings) override
    {
        if (GetBlock()) {
            // Blocks are not set for null columns.
            TBase::SetBlock(GetBlock(), GetSegmentMetas<TKeyMeta<Type>>());
        }

        for (auto [chunk, control] : matchings) {
            if (IsEmpty(control)) {
                nextMatchings->push_back({chunk, control});
                continue;
            }

            TBase::SetReadSpan(chunk);
            keys->SetReadSpan(control);

            BuildReadRowRanges(this, keys, nextMatchings);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

// Environment variable options for debug and benchmark purposes.
#ifdef FULL_UNPACK
static bool IsFullUnpack()
{
    static bool result = (getenv("FULL_UNPACK") != nullptr);
    return result;
}
#endif

static bool IsNoRead()
{
    static bool result = (getenv("NO_READ") != nullptr);
    return result;
}

class TRangeReader
    : public TRowsetBuilder<TReadSpan>
{
public:
    template <EValueType Type>
    struct TCreateRefiner
    {
        static std::unique_ptr<IColumnRefiner<TRowRange>> Do(const TColumnBase* columnInfo)
        {
            return std::make_unique<TColumnRefiner<TRowRange, Type>>(columnInfo);
        }
    };

    TRangeReader(
        TSharedRange<TRowRange> keyRanges,
        const TRowsetBuilderParams& params)
        : TRowsetBuilder<TReadSpan>(params, ECreateKeyReadersMode::ByIndexes)
        , KeyRanges_(std::move(keyRanges))
        , TableKeyColumnCount_(std::ssize(params.KeyTypes))
    {
        // ReadItemWidth are used only for range reads.
        for (int index = 0; index < static_cast<int>(params.ReadItemWidth); ++index) {
            ColumnRefiners_.push_back(DispatchByDataType<TCreateRefiner>(params.KeyTypes[index], &params.ColumnInfos[index]));
        }
    }

    bool IsReadListEmpty() const override
    {
        return ReadList_.empty();
    }

    ui32 ReadRowsByList(
        TMutableVersionedRow* rows,
        ui32 readCount,
        ui64* dataWeight,
        TReaderStatistics* readerStatistics) override
    {
#ifdef FULL_UNPACK
        if (IsFullUnpack()) {
            return DoReadRowsByListFull(rows, readCount, dataWeight, readerStatistics);
        }
#endif
        return DoReadRowsByListPartial(rows, readCount, dataWeight, readerStatistics);
    }

#ifdef FULL_UNPACK
    // Returns read row count.
    ui32 DoReadRowsByListFull(
        TMutableVersionedRow* rows,
        ui32 readCount,
        ui64* dataWeight,
        TReaderStatistics* readerStatistics)
    {
        ui32 segmentRowLimit = UpdateSegmentsFullUnpack(GetStartRowIndex(), readerStatistics);
        ui32 leftCount = readCount;

        TCpuDurationIncrementingGuard timingGuard(&readerStatistics->DoReadTime);

        auto spansIt = ReadList_.begin();

        // Ranges are limited by segmentRowLimit and readCountLimit

        // Last range [.........]
        //                ^       ^
        //                |       readCountLimit = leftCount + lower
        //                segmentRowLimit

        // Last range [.........]
        //               ^          ^
        //  readCountLimit          |
        //            segmentRowLimit

        // Last range [.........]
        //                ^   ^
        //                |   readCountLimit
        //                segmentRowLimit

        while (spansIt != ReadList_.end() && spansIt->Upper <= segmentRowLimit) {
            auto [lower, upper] = *spansIt;
            YT_VERIFY(lower != upper);

            if (lower + leftCount < upper) {
                break;
            }

            leftCount -= upper - lower;
            ++spansIt;
        }

        ui32 savedUpperBound = 0;
        if (spansIt != ReadList_.end() && spansIt->Lower < segmentRowLimit && leftCount > 0) {
            auto& [lower, upper] = *spansIt;
            ui32 splitBound = std::min(lower + leftCount, segmentRowLimit);
            YT_VERIFY(splitBound < upper);
            leftCount -= splitBound - lower;

            savedUpperBound = spansIt->Upper;
            YT_VERIFY(spansIt->Lower != splitBound);
            YT_VERIFY(spansIt->Upper != splitBound);
            spansIt->Upper = splitBound;
        }

        auto readListSlice = ReadList_.Slice(ReadList_.begin(), spansIt + (savedUpperBound > 0 ? 1 : 0));
        ReadList_ = ReadList_.Slice(spansIt, ReadList_.end());

        if (!IsNoRead()) {
            auto batchSize = this->ReadRowsWithoutKeys(rows, readListSlice, dataWeight, GetKeyColumnCount(), readerStatistics);

            {
                TCpuDurationIncrementingGuard timingGuard(&readerStatistics->DoReadKeysTime);
                ReadKeys(rows, readListSlice, batchSize, dataWeight);
            }

            SetRowsWithoutValuesToSentinels(rows, batchSize);
        }

        if (savedUpperBound > 0) {
            // Set spansIt->Lower = splitBound and restore spansIt->Upper.
            spansIt->Lower = spansIt->Upper;
            spansIt->Upper = savedUpperBound;
        }

        return readCount - leftCount;
    }
#endif

    // Returns read row count.
    ui32 DoReadRowsByListPartial(
        TMutableVersionedRow* rows,
        ui32 readCount,
        ui64* dataWeight,
        TReaderStatistics* readerStatistics)
    {
        ui32 segmentRowLimit = UpdateSegmentsPartialUnpack(CurrentResultOffset_, ReadList_, readerStatistics);
        ui32 leftCount = readCount;

        TCpuDurationIncrementingGuard timingGuard(&readerStatistics->DoReadTime);

        auto spansIt = ReadList_.begin();

        // Ranges are limited by segmentRowLimit and readCountLimit

        // Last range [.........]
        //                ^       ^
        //                |       readCountLimit = leftCount + lower
        //                segmentRowLimit

        // Last range [.........]
        //               ^          ^
        //  readCountLimit          |
        //            segmentRowLimit

        // Last range [.........]
        //                ^   ^
        //                |   readCountLimit
        //                segmentRowLimit

        while (spansIt != ReadList_.end() && spansIt->Upper <= segmentRowLimit) {
            auto [lower, upper] = *spansIt;
            YT_VERIFY(lower != upper);

            if (lower + leftCount < upper) {
                break;
            }

            leftCount -= upper - lower;
            ++spansIt;
        }

        if (spansIt != ReadList_.end() && spansIt->Lower < segmentRowLimit && leftCount > 0) {
            auto& [lower, upper] = *spansIt;
            ui32 splitBound = std::min(lower + leftCount, segmentRowLimit);
            YT_VERIFY(splitBound < upper);
            leftCount -= splitBound - lower;

            YT_VERIFY(spansIt->Lower != splitBound);
            YT_VERIFY(spansIt->Upper != splitBound);
            spansIt->Lower = splitBound;
        }

        ReadList_ = ReadList_.Slice(spansIt, ReadList_.end());

        if (!IsNoRead()) {
            TReadSpan readSpan{CurrentResultOffset_, CurrentResultOffset_ + readCount - leftCount};
            // TODO(lukyan): Use separate version for one range.
            // It will reduce function size and improve compiler optimizations.
            auto readList = MakeRange(&readSpan, 1);
            auto batchSize = this->ReadRowsWithoutKeys(rows, readList, dataWeight, GetKeyColumnCount(), readerStatistics);

            {
                TCpuDurationIncrementingGuard timingGuard(&readerStatistics->DoReadKeysTime);
                ReadKeys(rows, readList, batchSize, dataWeight);
            }

            SetRowsWithoutValuesToSentinels(rows, batchSize);
        }
        CurrentResultOffset_ += readCount - leftCount;

        return readCount - leftCount;
    }


private:
    TMemoryHolder<TReadSpan> ReadListHolder_;
    TMutableRange<TReadSpan> ReadList_;

    ui32 CurrentResultOffset_ = 0;

    const TSharedRange<TRowRange> KeyRanges_;
    const ui32 TableKeyColumnCount_;
    std::vector<std::unique_ptr<IColumnRefiner<TRowRange>>> ColumnRefiners_;

    TTmpBuffers TmpBuffers_;

    ui32 GetStartRowIndex() const
    {
        YT_VERIFY(!ReadList_.empty());
        return ReadList_.Front().Lower;
    }

#ifdef FULL_UNPACK
    ui32 UpdateSegmentsFullUnpack(ui32 rowIndex, TReaderStatistics* readerStatistics)
    {
        // Timestamp column segment limit.
        ui32 segmentRowLimit = TBase::GetSegmentRowLimit();
        if (rowIndex >= segmentRowLimit) {
            TCpuDurationIncrementingGuard timingGuard(&readerStatistics->DecodeTimestampSegmentTime);

            ++readerStatistics->UpdateSegmentCallCount;
            segmentRowLimit = TBase::UpdateSegment(rowIndex, &TmpBuffers_, NewMeta_);
        }

        {
            TCpuDurationIncrementingGuard timingGuard(&readerStatistics->DecodeKeySegmentTime);
            for (const auto& column : KeyColumns_) {
                auto currentLimit = column->GetSegmentRowLimit();
                if (rowIndex >= currentLimit) {
                    ++readerStatistics->UpdateSegmentCallCount;
                    currentLimit = column->UpdateSegment(rowIndex, &TmpBuffers_, NewMeta_);
                    Positions_[&column - KeyColumns_.begin()] = 0;
                }

                segmentRowLimit = std::min(segmentRowLimit, currentLimit);
            }
        }

        {
            TCpuDurationIncrementingGuard timingGuard(&readerStatistics->DecodeValueSegmentTime);
            for (const auto& column : ValueColumns_) {
                auto currentLimit = column->GetSegmentRowLimit();
                if (rowIndex >= currentLimit) {
                    ++readerStatistics->UpdateSegmentCallCount;
                    currentLimit = column->UpdateSegment(rowIndex, &TmpBuffers_, NewMeta_);
                    Positions_[GetKeyColumnCount() + &column - ValueColumns_.begin()] = 0;
                }

                segmentRowLimit = std::min(segmentRowLimit, currentLimit);
            }

        }

        return segmentRowLimit;
    }
#endif

    ui32 UpdateSegmentsPartialUnpack(ui32 resultRowOffset, TMutableRange<TReadSpan> spans, TReaderStatistics* readerStatistics)
    {
        YT_VERIFY(!spans.empty());
        ui32 startRowIndex = spans.Front().Lower;

        // Timestamp column segment limit.
        ui32 segmentRowLimit = TBase::GetSegmentRowLimit();
        if (startRowIndex >= segmentRowLimit) {
            TCpuDurationIncrementingGuard timingGuard(&readerStatistics->DecodeTimestampSegmentTime);

            ++readerStatistics->UpdateSegmentCallCount;
            segmentRowLimit = TBase::UpdateSegment(resultRowOffset, spans, &TmpBuffers_, NewMeta_);
        }

        {
            TCpuDurationIncrementingGuard timingGuard(&readerStatistics->DecodeKeySegmentTime);
            for (const auto& column : KeyColumns_) {
                auto currentLimit = column->GetSegmentRowLimit();
                if (startRowIndex >= currentLimit) {
                    ++readerStatistics->UpdateSegmentCallCount;
                    currentLimit = column->UpdateSegment(resultRowOffset, spans, &TmpBuffers_, NewMeta_);
                    Positions_[&column - KeyColumns_.begin()] = 0;
                }

                segmentRowLimit = std::min(segmentRowLimit, currentLimit);
            }
        }

        {
            TCpuDurationIncrementingGuard timingGuard(&readerStatistics->DecodeValueSegmentTime);
            for (const auto& column : ValueColumns_) {
                auto currentLimit = column->GetSegmentRowLimit();
                if (startRowIndex >= currentLimit) {
                    ++readerStatistics->UpdateSegmentCallCount;
                    currentLimit = column->UpdateSegment(resultRowOffset, spans, &TmpBuffers_, NewMeta_);
                    Positions_[GetKeyColumnCount() + &column - ValueColumns_.begin()] = 0;
                }

                segmentRowLimit = std::min(segmentRowLimit, currentLimit);
            }
        }

        return segmentRowLimit;
    }

    void BuildReadListForWindow(
        TSpanMatching initialWindow,
        const NTableClient::TKeyFilterStatisticsPtr& keyFilterStatistics) override
    {
        std::vector<TSpanMatching> matchings;
        std::vector<TSpanMatching> nextMatchings;

        auto [chunkSpan, controlSpan] = initialWindow;

        // Each range consists of two bounds.
        controlSpan.Lower *= 2;
        controlSpan.Upper *= 2;

        TRangeSliceAdapter keys;
        keys.Ranges = KeyRanges_;

        // All values must be accessible in column refiner.
        while (
            controlSpan.Lower < controlSpan.Upper &&
            keys.GetBound(controlSpan.Lower).GetCount() == 0)
        {
            // Bound is empty skip it.
            ++controlSpan.Lower;
        }

        // Inside column refiner empty range denotes universal range.
        if (!IsEmpty(controlSpan)) {
            matchings.push_back({chunkSpan, controlSpan});
        }

        for (ui32 columnId = 0; columnId < ColumnRefiners_.size(); ++columnId) {
            keys.ColumnId = columnId;
            keys.LastColumn = columnId + 1 == TableKeyColumnCount_;
            ColumnRefiners_[columnId]->Refine(&keys, matchings, &nextMatchings);
            matchings.clear();
            nextMatchings.swap(matchings);
        }

        ReadListHolder_.Resize(matchings.size());
        auto it = ReadListHolder_.GetData();

        i64 falsePositiveCount = 0;
        ui32 lastBound = SentinelRowIndex;
        for (const auto& [chunkSpan, controlSpan] : matchings) {
            YT_VERIFY(controlSpan.Lower == controlSpan.Upper ||
                controlSpan.Lower + 1 == controlSpan.Upper);

            falsePositiveCount += static_cast<i64>(IsEmpty(chunkSpan));

            if (chunkSpan.Lower == lastBound) {
                // Concat adjacent spans.
                it[-1].Upper = chunkSpan.Upper;
            } else if (!IsEmpty(chunkSpan)) {
                *it++ = chunkSpan;
            }
            lastBound = chunkSpan.Upper;
        }

        if (keyFilterStatistics && falsePositiveCount > 0) {
            keyFilterStatistics->FalsePositiveEntryCount.fetch_add(falsePositiveCount, std::memory_order::relaxed);
        }
        ReadList_ = MakeMutableRange(ReadListHolder_.GetData(), it);
    }
};

std::unique_ptr<IRowsetBuilder> CreateRowsetBuilder(
    TSharedRange<TRowRange> keyRanges,
    const TRowsetBuilderParams& params)
{
    return std::make_unique<TRangeReader>(std::move(keyRanges), params);
}

////////////////////////////////////////////////////////////////////////////////

class TLookupReader
    : public TRowsetBuilder<ui32>
{
public:
    template <EValueType Type>
    struct TCreateRefiner
    {
        static std::unique_ptr<IColumnRefiner<TLegacyKey>> Do(const TColumnBase* columnInfo)
        {
            return std::make_unique<TColumnRefiner<TLegacyKey, Type>>(columnInfo);
        }
    };

    TLookupReader(
        TSharedRange<TLegacyKey> keys,
        const TRowsetBuilderParams& params)
        : TRowsetBuilder<ui32>(params, ECreateKeyReadersMode::None)
        , Keys_(std::move(keys))
        , KeyColumnIndexes_(std::move(params.KeyColumnIndexes))
        , FixedDataWeightPartForKey_(0)
    {
        for (int index = 0; index < std::ssize(params.KeyTypes); ++index) {
            ColumnRefiners_.push_back(DispatchByDataType<TCreateRefiner>(params.KeyTypes[index], &params.ColumnInfos[index]));
        }

        for (auto keyColumnIndex : KeyColumnIndexes_) {
            auto keyType = params.KeyTypes[keyColumnIndex];
            if (IsStringLikeType(keyType)) {
                StringLikeKeyColumnIndexes_.push_back(keyColumnIndex);
            } else {
                FixedDataWeightPartForKey_ += GetDataWeight(keyType);
            }
        }
    }

    bool IsReadListEmpty() const override
    {
        return ReadList_.empty();
    }

    // Returns read row count.
    ui32 ReadRowsByList(
        TMutableVersionedRow* rows,
        ui32 readCount,
        ui64* dataWeight,
        TReaderStatistics* readerStatistics) override
    {
        ui32 segmentRowLimit = UpdateSegmentsNoUnpack(GetStartRowIndex(), readerStatistics);

        TCpuDurationIncrementingGuard timingGuard(&readerStatistics->DoReadTime);

        auto readRowsIt = ReadList_.begin();

        readRowsIt = LinearSearch(
            readRowsIt,
            readRowsIt + std::min<size_t>(readCount, ReadList_.size()),
            [=] (auto it) {
                ui32 rowIndex = *it;
                if (rowIndex == SentinelRowIndex) {
                    return true;
                }
                return rowIndex < segmentRowLimit;
            });

        auto readRowCount = readRowsIt - ReadList_.begin();

        auto readListSlice = ReadList_.Slice(ReadList_.begin(), readRowsIt);
        ReadList_ = ReadList_.Slice(readRowsIt, ReadList_.end());

        auto sentinelRowIndexes = Allocate<ui32>(GetPool(), readRowCount);
        auto spanEnd = BuildSentinelRowIndexes(readListSlice.begin(), readListSlice.end(), sentinelRowIndexes);
        auto sentinelRowIndexesCount = readListSlice.end() - spanEnd;

        // Make range to optimize access in TCompactVector.
        auto keyColumnIndexes = MakeRange(KeyColumnIndexes_);

        // Now all spans are not empty.
        auto batchSize = this->ReadRowsWithoutKeys(
            rows + sentinelRowIndexesCount,
            MakeRange(readListSlice.begin(), spanEnd),
            dataWeight,
            std::ssize(keyColumnIndexes),
            readerStatistics);

        SetRowsWithoutValuesToSentinels(rows + sentinelRowIndexesCount, batchSize);

        InsertSentinelRows(MakeRange(sentinelRowIndexes, sentinelRowIndexesCount), rows);

        for (int rowIndex = 0; rowIndex < readRowCount; ++rowIndex) {
            if (rows[rowIndex]) {
                auto keyRef = Keys_[StartKeyIndex_ + rowIndex].Elements();
                auto* rowKey = rows[rowIndex].BeginKeys();
                for (auto keyColumnIndex : keyColumnIndexes) {
                    *rowKey++ = keyRef[keyColumnIndex];
                }

                *dataWeight += FixedDataWeightPartForKey_;
                for (auto keyColumnIndex : StringLikeKeyColumnIndexes_) {
                    *dataWeight += keyRef[keyColumnIndex].Length;
                }
            }
        }

        StartKeyIndex_ += readRowCount;

        return readRowCount;
    }

private:
    TMemoryHolder<ui32> ReadListHolder_;
    TMutableRange<ui32> ReadList_;

    const TSharedRange<TLegacyKey> Keys_;
    const TCompactVector<ui16, 8> KeyColumnIndexes_;

    ui64 FixedDataWeightPartForKey_;
    TCompactVector<ui16, 8> StringLikeKeyColumnIndexes_;
    std::vector<std::unique_ptr<IColumnRefiner<TLegacyKey>>> ColumnRefiners_;

    int StartKeyIndex_;

    ui32 GetStartRowIndex() const
    {
        YT_VERIFY(!ReadList_.empty());

        for (auto rowIndex : ReadList_) {
            if (rowIndex != SentinelRowIndex) {
                return rowIndex;
            }
        }
        return 0;
    }

    ui32 UpdateSegmentsNoUnpack(ui32 rowIndex, TReaderStatistics* readerStatistics)
    {
        // Timestamp column segment limit.
        ui32 segmentRowLimit = TBase::GetSegmentRowLimit();
        if (rowIndex >= segmentRowLimit) {
            TCpuDurationIncrementingGuard timingGuard(&readerStatistics->DecodeTimestampSegmentTime);

            ++readerStatistics->UpdateSegmentCallCount;
            segmentRowLimit = TBase::UpdateSegment(rowIndex, NewMeta_);
        }

        {
            TCpuDurationIncrementingGuard timingGuard(&readerStatistics->DecodeValueSegmentTime);
            for (const auto& column : ValueColumns_) {
                auto currentLimit = column->GetSegmentRowLimit();
                if (rowIndex >= currentLimit) {
                    ++readerStatistics->UpdateSegmentCallCount;
                    currentLimit = column->UpdateSegment(rowIndex, NewMeta_);
                    Positions_[GetKeyColumnCount() + &column - ValueColumns_.begin()] = 0;
                }

                segmentRowLimit = std::min(segmentRowLimit, currentLimit);
            }

        }

        return segmentRowLimit;
    }

    void BuildReadListForWindow(
        TSpanMatching initialWindow,
        const NTableClient::TKeyFilterStatisticsPtr& /*keyFilterStatistics*/) override
    {
        // TODO(lukyan): Reuse vectors.
        std::vector<TSpanMatching> matchings;
        std::vector<TSpanMatching> nextMatchings;

        TBoundsIterator<TLegacyKey> keys;
        keys.Keys = Keys_;

        matchings.push_back(initialWindow);

        for (ui32 columnId = 0; columnId < ColumnRefiners_.size(); ++columnId) {
            keys.ColumnId = columnId;
            ColumnRefiners_[columnId]->Refine(&keys, matchings, &nextMatchings);

            matchings.clear();
            nextMatchings.swap(matchings);
        }

        auto initialControlSpan = initialWindow.Control;
        ReadListHolder_.Resize(initialControlSpan.Upper - initialControlSpan.Lower);
        auto it = ReadListHolder_.GetData();

        StartKeyIndex_ = initialControlSpan.Lower;

        // Encode non existent keys in chunk as read span SentinelRowIndex.
        auto offset = initialWindow.Control.Lower;
        for (const auto& [chunk, control] : matchings) {
            YT_VERIFY(control.Lower + 1 == control.Upper);

            while (offset < control.Lower) {
                *it++ = SentinelRowIndex;
                ++offset;
            }

            *it++ = chunk.Lower;
            offset = control.Upper;
        }

        while (offset < initialWindow.Control.Upper) {
            *it++ = SentinelRowIndex;
            ++offset;
        }

        ReadList_ = MakeMutableRange(ReadListHolder_.GetData(), it);

#ifndef NDEBUG
        for (size_t index = 1; index < ReadList_.size(); ++index) {
            if (ReadList_[index] == SentinelRowIndex) {
                continue;
            }

            YT_VERIFY(ReadList_[index] != ReadList_[index - 1]);
        }
#endif
    }

    static ui32* BuildSentinelRowIndexes(ui32* it, ui32* end, ui32* sentinelRowIndexes)
    {
        ui32 offset = 0;
        auto* spanDest = it;

        for (; it != end; ++it) {
            if (*it == SentinelRowIndex) {
                *sentinelRowIndexes++ = offset;
            } else {
                *spanDest++ = *it;
                ++offset;
            }
        }

        return spanDest;
    }

    static void InsertSentinelRows(TRange<ui32> sentinelRowIndexes, TMutableVersionedRow* rows)
    {
        auto destRows = rows;
        rows += sentinelRowIndexes.size();
        ui32 sourceOffset = 0;

        for (auto rowIndex : sentinelRowIndexes) {
            if (sourceOffset < rowIndex) {
                destRows = std::move(rows + sourceOffset, rows + rowIndex, destRows);
                sourceOffset = rowIndex;
            }
            *destRows++ = TMutableVersionedRow();
        }
    }
};

std::unique_ptr<IRowsetBuilder> CreateRowsetBuilder(
    TSharedRange<TLegacyKey> keys,
    const TRowsetBuilderParams& params)
{
    return std::make_unique<TLookupReader>(std::move(keys), params);
}

////////////////////////////////////////////////////////////////////////////////

class TKeysWithHintsReader
    : public TRowsetBuilder<ui32>
{
public:
    TKeysWithHintsReader(
        TKeysWithHints keysWithHints,
        const TRowsetBuilderParams& params)
        : TRowsetBuilder<ui32>(params, ECreateKeyReadersMode::All)
        , KeysWithHints_(std::move(keysWithHints))
        , KeyColumnIndexes_(std::move(params.KeyColumnIndexes))
        , FixedDataWeightPartForKey_(0)
    {
        for (auto keyColumnIndex : KeyColumnIndexes_) {
            auto keyType = params.KeyTypes[keyColumnIndex];
            if (IsStringLikeType(keyType)) {
                StringLikeKeyColumnIndexes_.push_back(keyColumnIndex);
            } else {
                FixedDataWeightPartForKey_ += GetDataWeight(keyType);
            }
        }
    }

    bool IsReadListEmpty() const override
    {
        return ReadList_.empty();
    }

    // Returns read row count.
    ui32 ReadRowsByList(
        TMutableVersionedRow* rows,
        ui32 readCount,
        ui64* dataWeight,
        TReaderStatistics* readerStatistics) override
    {
        TCpuDurationIncrementingGuard timingGuard(&readerStatistics->DoReadTime);

        const auto* rowsEnd = rows + readCount;
        const auto* readListIt = ReadList_.begin();

        // Make range to optimize access to TCompactVector.
        auto keyColumnIndexes = MakeRange(KeyColumnIndexes_);

        for (; readListIt != ReadList_.end(); ++readListIt) {
            auto [rowIndexHint, keyIndex] = *readListIt;

            if (keyIndex < NextKeyIndex_) {
                continue;
            }

            if (Y_UNLIKELY(rowIndexHint == SentinelRowIndex)) {
                while (NextKeyIndex_ < keyIndex && rows != rowsEnd) {
                    *rows++ = TMutableVersionedRow();
                    ++NextKeyIndex_;
                }

                if (rows == rowsEnd) {
                    break;
                }

                YT_ASSERT(NextKeyIndex_ == keyIndex);

                *rows++ = TMutableVersionedRow();
                ++NextKeyIndex_;
                continue;
            }

            auto keyRef = ToKeyRef(KeysWithHints_.Keys[keyIndex]);
            if (KeyAtIndexExists(rowIndexHint, keyRef)) {
                while (NextKeyIndex_ < keyIndex && rows != rowsEnd) {
                    *rows++ = TMutableVersionedRow();
                    ++NextKeyIndex_;
                }

                if (rows == rowsEnd) {
                    break;
                }

                YT_ASSERT(NextKeyIndex_ == keyIndex);

                auto row = ReadRowWithoutKeys(rowIndexHint, dataWeight, std::ssize(keyColumnIndexes), readerStatistics);

                if (row) {
                    TCpuDurationIncrementingGuard timingGuard(&readerStatistics->DoReadKeysTime);
                    auto* rowKey = row.BeginKeys();
                    for (auto keyColumnIndex : keyColumnIndexes) {
                        *rowKey++ = keyRef[keyColumnIndex];
                    }

                    *dataWeight += FixedDataWeightPartForKey_;
                    for (auto keyColumnIndex : StringLikeKeyColumnIndexes_) {
                        *dataWeight += keyRef[keyColumnIndex].Length;
                    }
                }

                *rows++ = row;
                ++NextKeyIndex_;
            }
        }

        ReadList_ = MakeRange(readListIt, ReadList_.end());

        return readCount - (rowsEnd - rows);
    }

private:
    TKeysWithHints KeysWithHints_;
    const TCompactVector<ui16, 8> KeyColumnIndexes_;

    ui64 FixedDataWeightPartForKey_;
    TCompactVector<ui16, 8> StringLikeKeyColumnIndexes_;

    TRange<std::pair<ui32, ui32>> ReadList_;
    ui32 NextKeyIndex_ = 0;

    std::vector<TReadSpan> ValueIndexes_;

    bool KeyAtIndexExists(ui32 rowIndexHint, NTableClient::TKeyRef keyRef)
    {
        YT_VERIFY(NewMeta_);

        const auto* keyRefIt = keyRef.Begin();
        for (const auto& column : KeyColumns_) {
            if (!column->HasKeyAtIndex(*keyRefIt, rowIndexHint)) {
                return false;
            }

            ++keyRefIt;
        }
        return true;
    }

    TMutableVersionedRow ReadRowWithoutKeys(
        ui32 rowIndex,
        ui64* dataWeight,
        int targetKeyColumnCount,
        TReaderStatistics* readerStatistics)
    {
        ++readerStatistics->DoReadCallCount;

        YT_VERIFY(NewMeta_);

        TValueOutput valueOutput;
        TMutableVersionedRow row;

        ValueIndexes_.resize(ValueColumns_.size());

        ui32 valueCount = 0;
        {
            TCpuDurationIncrementingGuard timingGuard(&readerStatistics->DecodeValueSegmentTime);
            readerStatistics->UpdateSegmentCallCount += std::ssize(ValueColumns_);

            for (int index = 0; index < std::ssize(ValueColumns_); ++index) {
                ValueColumns_[index]->InitAndPrefetch(rowIndex);
            }
        }

        {
            TCpuDurationIncrementingGuard timingGuard(&readerStatistics->CollectCountsTime);

            for (int index = 0; index < std::ssize(ValueColumns_); ++index) {
                auto valueSpan = ValueColumns_[index]->SkipToValueAndPrefetch(rowIndex);
                ValueIndexes_[index] = valueSpan;
                valueCount += valueSpan.Upper - valueSpan.Lower;
            }
        }

        {
            TCpuDurationIncrementingGuard timingGuard(&readerStatistics->AllocateRowsTime);

            TBase::UpdateSegment(rowIndex, NewMeta_);
            row = TRowsetBuilder<ui32>::DoAllocateRow(
                &MemoryPool_,
                &valueOutput,
                targetKeyColumnCount,
                valueCount,
                rowIndex);
        }

        if (row.GetDeleteTimestampCount() == 0 && row.GetWriteTimestampCount() == 0) {
            // Key is present in chunk but no values corresponding to requested timestamp.
            return {};
        }

        {
            TCpuDurationIncrementingGuard timingGuard(&readerStatistics->DoReadValuesTime);
            for (int index = 0; index < std::ssize(ValueColumns_); ++index) {
                *dataWeight += ValueColumns_[index]->ReadValue(&valueOutput, ValueIndexes_[index]);
            }
        }

        row.SetValueCount(valueOutput.Ptr - row.BeginValues());

        *dataWeight += row.GetWriteTimestampCount() * sizeof(TTimestamp);
        *dataWeight += row.GetDeleteTimestampCount() * sizeof(TTimestamp);

        return row;
    }

    void BuildReadListForWindow(
        TSpanMatching initialWindow,
        const NTableClient::TKeyFilterStatisticsPtr& /*keyFilterStatistics*/) override
    {
        auto initialControlSpan = initialWindow.Control;
        ReadList_ = MakeMutableRange(KeysWithHints_.RowIndexesToKeysIndexes)
            .Slice(initialControlSpan.Lower, initialControlSpan.Upper);
    }
};

std::unique_ptr<IRowsetBuilder> CreateRowsetBuilder(
    TKeysWithHints keysWithHints,
    const TRowsetBuilderParams& params)
{
    return std::make_unique<TKeysWithHintsReader>(std::move(keysWithHints), params);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
