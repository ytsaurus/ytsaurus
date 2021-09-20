#include "rowset_builder.h"
#include "read_span_refiner.h"
#include "dispatch_by_type.h"
#include "reader_statistics.h"

namespace NYT::NNewTableClient {

using NTableClient::NullTimestamp;

using NProfiling::TValueIncrementingTimingGuard;
using NProfiling::TWallTimer;

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
    return IsStringLikeType(Type) ? value.Length * count : 0;
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

    virtual ui32 UpdateSegment(ui32 rowIndex, TTmpBuffers* tmpBuffers) = 0;

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
{
public:
    explicit TKeyColumn(const TColumnBase* columnInfo)
        : TKeyColumnBase<TReadSpan>(columnInfo)
    {
        if (columnInfo->IsNull()) {
            // Key column not present in chunk.
            TKeyColumnBase<TReadSpan>::InitNull();
            Value_.InitNull();
        }
    }

    ui32 UpdateSegment(ui32 rowIndex, TTmpBuffers* tmpBuffers) override
    {
        auto meta = SkipToSegment<TKeyMeta<Type>>(rowIndex);
        if (meta) {
            auto data = GetBlock().begin() + meta->Offset;
            DoInitSegment(&Value_, this, meta, reinterpret_cast<const ui64*>(data), tmpBuffers);
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
            position = DoReadKeys(keys, lower, upper, position, columnId, dataWeight);
            keys += upper - lower;
        }

        return position;
    }

private:
    TScanDataExtractor<Type> Value_;

    Y_FORCE_INLINE ui32 DoReadKeys(
        TUnversionedValue** keys,
        ui32 rowIndex,
        ui32 rowLimit,
        ui32 position,
        ui16 columnId,
        ui64* dataWeight) const
    {
        YT_ASSERT(rowLimit <= GetSegmentRowLimit());

        position = SkipTo(rowIndex, position);

        YT_ASSERT(position < GetCount());
        YT_ASSERT(rowIndex < UpperRowBound(position));

        // Keep counter in register.
        ui64 localDataWeight = 0;
        while (rowIndex < rowLimit) {
            TUnversionedValue value{};
            value.Id = columnId;

            YT_ASSERT(position < GetCount());
            Value_.Extract(&value, position);

            ui32 nextRowIndex = rowLimit < UpperRowBound(position) ? rowLimit : UpperRowBound(position++);

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

    virtual ui32 UpdateSegment(ui32 rowIndex, TTmpBuffers* tmpBuffers) = 0;

    virtual ui32 ReadKeys(
        TUnversionedValue** keys,
        TRange<ui32> readIndexes,
        ui32 position,
        ui16 columnId,
        ui64* dataWeight) const = 0;
};

template <EValueType Type>
class TKeyColumn<ui32, Type>
    : public TKeyColumnBase<ui32>
{
public:
    explicit TKeyColumn(const TColumnBase* columnInfo)
        : TKeyColumnBase<ui32>(columnInfo)
    {
        if (columnInfo->IsNull()) {
            // Key column not present in chunk.
            TKeyColumnBase<ui32>::InitNull();
            Value_.InitNull();
        }
    }

    ui32 UpdateSegment(ui32 rowIndex, TTmpBuffers*) override
    {
        auto meta = SkipToSegment<TKeyMeta<Type>>(rowIndex);
        if (meta) {
            auto data = GetBlock().begin() + meta->Offset;
            DoInitSegment(&Value_, this, meta, reinterpret_cast<const ui64*>(data));
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
            value.Id = columnId;

            YT_ASSERT(position < GetCount());
            Value_.Extract(&value, position);

            localDataWeight += GetFixedDataWeightPart<Type>(1) +
                GetVariableDataWeightPart<Type>(value);

            (*keys++)[columnId] = value;
        }

        *dataWeight += localDataWeight;

        return position;
    }

private:
    TLookupDataExtractor<Type> Value_;
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

    explicit TVersionedValueReader(ui16 columnId)
        : ColumnId_(columnId)
    { }

    void Init(const TMetaBase* meta, const ui64* data, TTmpBuffers* tmpBuffers)
    {
        data = TScanVersionExtractor<Aggregate>::Init(data);
        TScanDataExtractor<Type>::Init(meta, data, tmpBuffers);
    }

protected:
    const ui16 ColumnId_;
};

template <EValueType Type, bool Aggregate>
class TVersionedValueLookuper
    : public TLookupVersionExtractor<Aggregate>
    , public TLookupDataExtractor<Type>
{
public:
    static constexpr bool Aggregate_ = Aggregate;
    static constexpr EValueType Type_ = Type;

    explicit TVersionedValueLookuper(ui16 columnId)
        : ColumnId_(columnId)
    { }

    void Init(const TMetaBase* meta, const ui64* data)
    {
        data = TLookupVersionExtractor<Aggregate>::Init(data);
        TLookupDataExtractor<Type>::Init(meta, data);
    }

protected:
    const ui16 ColumnId_;
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
    using TBase::ColumnId_;

    // Returns data weight.
    Y_FORCE_INLINE ui64 operator() (
        ui32 valueIdx,
        ui32 valueIdxEnd,
        TValueOutput* valueOutput) const
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
            valuePtr->Id = ColumnId_;
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
    TValueColumnBase(const TColumnBase* columnInfo, bool aggregate)
        : TColumnBase(columnInfo)
        , Aggregate_(aggregate)
    { }

    virtual ~TValueColumnBase() = default;

    virtual ui32 UpdateSegment(ui32 rowIndex, TTmpBuffers* tmpBuffers) = 0;

    ui32 CollectCounts(ui32* counts, TRange<ui32> readIndexes, ui32 position)
    {
        // Initialize lazily.
        const_cast<TValueColumnBase*>(this)->DoInitialize();
        return DoCollectCounts_(this, counts, readIndexes, position);
    }

    ui32 ReadValues(
        TValueOutput* valueOutput,
        TRange<ui32> readIndexes,
        ui32 position,
        ui64* dataWeight) const
    {
        // Initialize lazily.
        const_cast<TValueColumnBase*>(this)->DoInitialize();
        return DoReadValues_(this, valueOutput, readIndexes, position, dataWeight);
    }

    bool IsAggregate() const
    {
        return Aggregate_;
    }

protected:
    ui32 (*DoCollectCounts_)(const TLookupMultiValueIndexExtractor*, ui32*, TRange<ui32>, ui32) = nullptr;

    ui32 (*DoReadValues_)(
        const TLookupMultiValueIndexExtractor*,
        TValueOutput* valueOutput,
        TRange<ui32> readIndexes,
        ui32 position,
        ui64* dataWeight) = nullptr;

    // This field allows to check aggregate flag without calling virtual method.
    const bool Aggregate_;

    virtual void DoInitialize() = 0;
};

template <EValueType Type, bool Aggregate, bool ProduceAll>
class TVersionedValueColumn<ui32, Type, Aggregate, ProduceAll>
    : public TValueColumnBase<ui32>
    , public TVersionedValueLookuper<Type, Aggregate>
{
public:
    using TVersionedValueBase = TVersionedValueLookuper<Type, Aggregate>;

    TVersionedValueColumn(const TColumnBase* columnInfo, ui16 columnId)
        : TValueColumnBase<ui32>(columnInfo, Aggregate)
        , TVersionedValueBase(columnId)
    { }

    ui32 UpdateSegment(ui32 rowIndex, TTmpBuffers*) override
    {
        auto meta = SkipToSegment<TValueMeta<Type>>(rowIndex);
        if (!meta) {
            return GetSegmentRowLimit();
        }
        Meta_ = meta;

#ifdef USE_UNSPECIALIZED_SEGMENT_READERS
        DoCollectCounts_ = &CallCastedMixin<
            const TCountsCollector<TLookupMultiValueIndexExtractor>,
            const TLookupMultiValueIndexExtractor,
            ui32*,
            TRange<ui32>,
            ui32>;

        DoReadValues_ = &DoReadValues<TLookupMultiValueIndexExtractor, TVersionedValueBase>;
#else
        bool dense = IsDense(meta->Type);
        if (dense) {
            InitSegmentReaders<true>(meta->Type);
        } else {
            InitSegmentReaders<false>(meta->Type);
        }
#endif

        return meta->ChunkRowCount;
    }

    void DoInitialize() override
    {
        if (Meta_) {
            bool dense = IsDense(Meta_->Type);
            auto data = GetBlock().begin() + Meta_->Offset;
            auto ptr = TValueColumnBase<ui32>::Init(Meta_, Meta_, reinterpret_cast<const ui64*>(data), dense);
            TVersionedValueBase::Init(Meta_, ptr);
            Meta_ = nullptr;
        }
    }

    template <bool Dense>
    void InitSegmentReaders(int metaType)
    {
        using TIndexExtractorBase = TSpecializedMultiValueIndexLookuper<Dense>;

        DoCollectCounts_ = &CallCastedMixin<
            const TCountsCollector<TIndexExtractorBase>,
            const TLookupMultiValueIndexExtractor,
            ui32*,
            TRange<ui32>,
            ui32>;

        if constexpr (Type == EValueType::Double || Type == EValueType::Boolean) {
            DoReadValues_ = &DoReadValues<TIndexExtractorBase, TVersionedValueBase>;
        } else {
            DoReadValues_ = IsDirect(metaType)
                ? &DoReadValues<TIndexExtractorBase, TSpecializedVersionedValueLookuper<Type, Aggregate, true>>
                : &DoReadValues<TIndexExtractorBase, TSpecializedVersionedValueLookuper<Type, Aggregate, false>>;
        }
    }

    template <class TIndexExtractorBase, class TVersionedValueExtractorBase>
    static ui32 DoReadValues(
        const TLookupMultiValueIndexExtractor* base,
        TValueOutput* valueOutput,
        TRange<ui32> readIndexes,
        ui32 position,
        ui64* dataWeight)
    {
        // Keep counter in register.
        ui64 localDataWeight = 0;
        for (auto readIndex : readIndexes) {
            position = CallCastedMixin<const TSkipperTo<TIndexExtractorBase>, const TLookupMultiValueIndexExtractor>
                (base, readIndex, position);
            ui32 valueIdx = position;
            position = CallCastedMixin<const TSkipperTo<TIndexExtractorBase>, const TLookupMultiValueIndexExtractor>
                (base, readIndex + 1, position);
            ui32 valueIdxEnd = position;

            localDataWeight += CallCastedMixin<
                const TVersionedValueExtractor<TVersionedValueExtractorBase, ProduceAll>,
                const TVersionedValueBase>
                (static_cast<const TVersionedValueColumn*>(base), valueIdx, valueIdxEnd, valueOutput);

            ++valueOutput;
        }
        *dataWeight += localDataWeight;

        return position;
    }

private:
    const TValueMeta<Type>* Meta_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

template <>
class TValueColumnBase<TReadSpan>
    : public TColumnBase
    , public TScanMultiValueIndexExtractor
{
public:
    TValueColumnBase(const TColumnBase* columnInfo, bool aggregate)
        : TColumnBase(columnInfo)
        , Aggregate_(aggregate)
    { }

    virtual ~TValueColumnBase() = default;

    virtual ui32 UpdateSegment(ui32 rowIndex, TTmpBuffers* tmpBuffers) = 0;

    ui32 CollectCounts(
        ui32* counts,
        TRange<TReadSpan> spans,
        ui32 position) const
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

    TVersionedValueColumn(const TColumnBase* columnInfo, ui16 columnId)
        : TValueColumnBase<TReadSpan>(columnInfo, Aggregate)
        , TVersionedValueBase(columnId)
    { }

    ui32 UpdateSegment(ui32 rowIndex, TTmpBuffers* tmpBuffers) override
    {
        auto meta = SkipToSegment<TValueMeta<Type>>(rowIndex);

        if (meta) {
            auto data = GetBlock().begin() + meta->Offset;

            bool dense = IsDense(meta->Type);
            auto ptr = TValueColumnBase<TReadSpan>::Init(meta, meta, reinterpret_cast<const ui64*>(data), dense, tmpBuffers);
            TVersionedValueBase::Init(meta, ptr, tmpBuffers);
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
                (this, valueIdx, valueIdxEnd, valueOutput);

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
{ };

template <>
struct TTimestampExtractor<ui32>
    : public TLookupTimestampExtractor
{ };

////////////////////////////////////////////////////////////////////////////////

template <class TReadItem>
class TRowAllocatorBase
    : public TTimestampExtractor<TReadItem>
    , public TColumnBase
{
public:
    using TTimestampExtractorBase = TTimestampExtractor<TReadItem>;

    using TTimestampExtractorBase::GetSegmentRowLimit;
    using TTimestampExtractorBase::ReadSegment;
    using TTimestampExtractorBase::GetWriteTimestamps;
    using TTimestampExtractorBase::GetDeleteTimestamps;

    TRowAllocatorBase(
        const TColumnBase* timestampColumnInfo,
        TTimestamp timestamp,
        bool produceAll)
        : TColumnBase(timestampColumnInfo)
        , Timestamp_(timestamp)
        , ProduceAll_(produceAll)
    { }

    ui32 UpdateSegment(ui32 rowIndex, TTmpBuffers* tmpBuffers)
    {
        auto meta = SkipToSegment<TTimestampMeta>(rowIndex);
        if (meta) {
            auto data = GetBlock().begin() + meta->Offset;
            ReadSegment(meta, data, tmpBuffers);
        }
        return GetSegmentRowLimit();
    }

    TMutableVersionedRow DoAllocateRow(
        NTableClient::TRowBuffer* rowBuffer,
        TValueOutput* valueOutput,
        ui32 keySize,
        ui32 valueCount,
        ui32 rowIndex) const
    {
        auto writeTimestamps = GetWriteTimestamps(rowIndex, rowBuffer->GetPool());
        auto deleteTimestamps = GetDeleteTimestamps(rowIndex, rowBuffer->GetPool());

        auto [lowerDeleteIt, lowerWriteIt] = GetLowerTimestampsIndexes(
            deleteTimestamps.Begin(),
            deleteTimestamps.End(),
            writeTimestamps.Begin(),
            writeTimestamps.End(),
            Timestamp_);

        // COMPAT(lukyan): Produce really all versions or all versions after last delete.
        if (ProduceAll_) {
            // Produces all versions and all delete timestamps.
            auto timestampIdRange = std::make_pair(
                lowerWriteIt - writeTimestamps.Begin(),
                writeTimestamps.Size());

            auto row = rowBuffer->AllocateVersioned(
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

            auto timestampIdRange = std::make_pair(
                lowerWriteIt - writeTimestamps.Begin(),
                upperWriteIt - writeTimestamps.Begin());

            auto row = rowBuffer->AllocateVersioned(
                keySize,
                valueCount,
                upperWriteIt != lowerWriteIt ? 1 : 0,
                deleteTimestamp != NullTimestamp ? 1 : 0);

            if (lowerWriteIt != upperWriteIt) {
                row.BeginWriteTimestamps()[0] = *lowerWriteIt;
            }

            if (deleteTimestamp != NullTimestamp) {
                row.BeginDeleteTimestamps()[0] = deleteTimestamp;
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

        return std::make_pair(lowerDeleteIt, lowerWriteIt);
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

template <class TReadItem>
class TRowsetBuilder
    : public IRowsetBuilder
    , public TRowAllocatorBase<TReadItem>
{
public:
    using TBase = TRowAllocatorBase<TReadItem>;
    using TBase::ProduceAll_;
    using TBase::Timestamp_;

    template <EValueType Type>
    struct TCreateKeyColumn
    {
        static std::unique_ptr<TKeyColumnBase<TReadItem>> Do(const TColumnBase* columnInfo)
        {
            return std::make_unique<TKeyColumn<TReadItem, Type>>(columnInfo);
        }
    };

    template <EValueType Type>
    struct TCreateVersionedValueColumn
    {
        template <bool Aggregate>
        static std::unique_ptr<TValueColumnBase<TReadItem>> DoInner(
            const TColumnBase* columnInfo,
            ui16 columnId,
            bool produceAll)
        {
            if (produceAll) {
                return std::make_unique<TVersionedValueColumn<TReadItem, Type, Aggregate, true>>(columnInfo, columnId);
            } else {
                return std::make_unique<TVersionedValueColumn<TReadItem, Type, Aggregate, false>>(columnInfo, columnId);
            }
        }

        static std::unique_ptr<TValueColumnBase<TReadItem>> Do(
            const TColumnBase* columnInfo,
            ui16 columnId,
            bool aggregate,
            bool produceAll)
        {
            if (aggregate) {
                return DoInner<true>(columnInfo, columnId, produceAll);
            } else {
                return DoInner<false>(columnInfo, columnId, produceAll);
            }
        }
    };

    TRowsetBuilder(
        TRange<EValueType> keyTypes,
        TRange<TValueSchema> valueSchema,
        TRange<TColumnBase> columnInfos,
        TTimestamp timestamp,
        bool produceAll)
        : TBase(&columnInfos.Back(), timestamp, produceAll)
    {
        auto columnInfoIt = columnInfos.begin();
        for (auto type : keyTypes) {
            KeyColumns_.push_back(DispatchByDataType<TCreateKeyColumn>(type, columnInfoIt++));
        }

        for (auto [type, columnId, aggregate] : valueSchema) {
            ValueColumns_.push_back(
                DispatchByDataType<TCreateVersionedValueColumn>(type, columnInfoIt++, columnId, aggregate, produceAll));
        }

        Positions_.Resize(keyTypes.size() + valueSchema.size());
        memset(Positions_.GetData(), 0, sizeof(ui32) * (keyTypes.size() + valueSchema.size()));
    }

    ui32 UpdateSegments(ui32 rowIndex, TReaderStatistics* readerStatistics)
    {
        // Timestamp column segment limit.
        ui32 segmentRowLimit = TBase::GetSegmentRowLimit();
        if (rowIndex >= segmentRowLimit) {
            TValueIncrementingTimingGuard<TWallTimer> timingGuard(&readerStatistics->DecodeTimestampSegmentTime);

            ++readerStatistics->UpdateSegmentCallCount;
            segmentRowLimit = TBase::UpdateSegment(rowIndex, &TmpBuffers_);
        }

        {
            TValueIncrementingTimingGuard<TWallTimer> timingGuard(&readerStatistics->DecodeKeySegmentTime);
            for (const auto& column : KeyColumns_) {
                auto currentLimit = column->GetSegmentRowLimit();
                if (rowIndex >= currentLimit) {
                    ++readerStatistics->UpdateSegmentCallCount;
                    currentLimit = column->UpdateSegment(rowIndex, &TmpBuffers_);
                    Positions_[&column - KeyColumns_.begin()] = 0;
                }

                segmentRowLimit = std::min(segmentRowLimit, currentLimit);
            }
        }

        {
            TValueIncrementingTimingGuard<TWallTimer> timingGuard(&readerStatistics->DecodeValueSegmentTime);
            for (const auto& column : ValueColumns_) {
                auto currentLimit = column->GetSegmentRowLimit();
                if (rowIndex >= currentLimit) {
                    ++readerStatistics->UpdateSegmentCallCount;
                    currentLimit = column->UpdateSegment(rowIndex, &TmpBuffers_);
                    Positions_[GetKeyColumnCount() + &column - ValueColumns_.begin()] = 0;
                }

                segmentRowLimit = std::min(segmentRowLimit, currentLimit);
            }

        }

        return segmentRowLimit;
    }

    ui16 GetKeyColumnCount() const
    {
        return KeyColumns_.size();
    }

    void ReadRows(
        TMutableVersionedRow* rows,
        TRange<TReadItem> readList,
        ui64* dataWeight,
        TReaderStatistics* readerStatistics)
    {
        ++readerStatistics->DoReadCallCount;
        if (readList.Empty()) {
            return;
        }
        ui32 batchSize = GetRowCount(readList);

        ValueCounts_.Resize(batchSize);
        auto valueCounts = ValueCounts_.GetData();
        std::fill_n(valueCounts, batchSize, 0);

        {
            TValueIncrementingTimingGuard<TWallTimer> timingGuard(&readerStatistics->CollectCountsTime);
            ui32 fixedValueCountColumns = 0;
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
            TValueIncrementingTimingGuard<TWallTimer> timingGuard(&readerStatistics->AllocateRowsTime);
            AllocateRows(rows, valueOutput, GetKeyColumnCount(), valueCounts, readList);
        }

        {
            TValueIncrementingTimingGuard<TWallTimer> timingGuard(&readerStatistics->DoReadKeysTime);
            ReadKeys(rows, readList, batchSize, dataWeight);
        }

        {
            TValueIncrementingTimingGuard<TWallTimer> timingGuard(&readerStatistics->DoReadValuesTime);
            ReadValues(rows, valueOutput, readList, batchSize, dataWeight);
        }
    }

    TChunkedMemoryPool* GetPool() const override
    {
        return Buffer_->GetPool();
    }

    void ClearBuffer() override
    {
        Buffer_->Clear();
    }

private:
    std::vector<std::unique_ptr<TKeyColumnBase<TReadItem>>> KeyColumns_;
    std::vector<std::unique_ptr<TValueColumnBase<TReadItem>>> ValueColumns_;

    const NTableClient::TRowBufferPtr Buffer_ = New<NTableClient::TRowBuffer>(TDataBufferTag());

    // Positions in segments are kept separately to minimize write memory footprint.
    // Column readers are immutable during read.
    TMemoryHolder<ui32> Positions_;

    TTmpBuffers TmpBuffers_;

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
                Buffer_.Get(),
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

            if (rows->GetDeleteTimestampCount() == 0 &&
                rows->GetWriteTimestampCount() == 0 &&
                Timestamp_ != NTableClient::AllCommittedTimestamp)
            {
                // Key is present in chunk but no values corresponding to requested timestamp.
                *rows = TMutableVersionedRow();
            } else {
                auto valuesIt = rows->BeginValues();
                auto valuesEnd = valueOutput->Ptr;
                rows->SetValueCount(valuesEnd - valuesIt);
            }

            ++valueOutput;
            ++rows;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 SentinelRowIndex = -1;

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

    explicit TColumnRefiner(const TColumnBase* columnInfo)
        : TColumnBase(columnInfo)
    { }

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

template <class TReadItem>
class TReader;

template <>
class TReader<TReadSpan>
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

    TReader(
        TSharedRange<TRowRange> keyRanges,
        TRange<EValueType> keyTypes,
        TRange<TValueSchema> valueSchema,
        TRange<TColumnBase> columnInfos,
        TTimestamp timestamp,
        bool produceAll)
        : TRowsetBuilder<TReadSpan>(keyTypes, valueSchema, columnInfos, timestamp, produceAll)
        , KeyRanges_(keyRanges)
    {
        for (int index = 0; index < std::ssize(keyTypes); ++index) {
            ColumnRefiners_.push_back(DispatchByDataType<TCreateRefiner>(keyTypes[index], &columnInfos[index]));
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
        ui32 segmentRowLimit = this->UpdateSegments(GetStartRowIndex(), readerStatistics);
        ui32 leftCount = readCount;

        TValueIncrementingTimingGuard<TWallTimer> timingGuard(&readerStatistics->DoReadTime);

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

        this->ReadRows(rows, readListSlice, dataWeight, readerStatistics);

        if (savedUpperBound > 0) {
            // Set spansIt->Lower = splitBound and restore spansIt->Upper.
            spansIt->Lower = spansIt->Upper;
            spansIt->Upper = savedUpperBound;
        }

        return readCount - leftCount;
    }

private:
    TMemoryHolder<TReadSpan> ReadListHolder_;
    TMutableRange<TReadSpan> ReadList_;

    const TSharedRange<TRowRange> KeyRanges_;
    std::vector<std::unique_ptr<IColumnRefiner<TRowRange>>> ColumnRefiners_;

    ui32 GetStartRowIndex() const
    {
        YT_VERIFY(!ReadList_.empty());
        return ReadList_.Front().Lower;
    }

    void BuildReadListForWindow(TSpanMatching initialWindow) override
    {
        std::vector<TSpanMatching> matchings;
        std::vector<TSpanMatching> nextMatchings;

        // Each range consists of two bounds.
        initialWindow.Control.Lower *= 2;
        initialWindow.Control.Upper *= 2;

        TRangeSliceAdapter keys;
        keys.Ranges = KeyRanges_;

        // All values must be accessible in column refiner.
        if (!keys.GetBound(initialWindow.Control.Lower).GetCount()) {
            // Bound is empty skip it.
            ++initialWindow.Control.Lower;
        }

        matchings.push_back(initialWindow);

        for (ui32 columnId = 0; columnId < ColumnRefiners_.size(); ++columnId) {
            keys.ColumnId = columnId;
            keys.LastColumn = columnId + 1 == ColumnRefiners_.size();
            ColumnRefiners_[columnId]->Refine(&keys, matchings, &nextMatchings);
            matchings.clear();
            nextMatchings.swap(matchings);
        }

        ReadListHolder_.Resize(matchings.size());
        auto it = ReadListHolder_.GetData();

        ui32 lastBound = SentinelRowIndex;
        for (const auto& [chunkSpan, controlSpan] : matchings) {
            YT_VERIFY(controlSpan.Lower == controlSpan.Upper ||
                controlSpan.Lower + 1 == controlSpan.Upper);

            if (chunkSpan.Lower == lastBound) {
                // Concat adjacent spans.
                it[-1].Upper = chunkSpan.Upper;
            } else if (!IsEmpty(chunkSpan)) {
                *it++ = chunkSpan;
            }
            lastBound = chunkSpan.Upper;
        }

        ReadList_ = MakeMutableRange(ReadListHolder_.GetData(), it);
    }
};

template <>
class TReader<ui32>
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

    TReader(
        TSharedRange<TLegacyKey> keys,
        TRange<EValueType> keyTypes,
        TRange<TValueSchema> valueSchema,
        TRange<TColumnBase> columnInfos,
        TTimestamp timestamp,
        bool produceAll)
        : TRowsetBuilder<ui32>(keyTypes, valueSchema, columnInfos, timestamp, produceAll)
        , Keys_(keys)
    {
        for (int index = 0; index < std::ssize(keyTypes); ++index) {
            ColumnRefiners_.push_back(DispatchByDataType<TCreateRefiner>(keyTypes[index], &columnInfos[index]));
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
        ui32 segmentRowLimit = this->UpdateSegments(GetStartRowIndex(), readerStatistics);

        TValueIncrementingTimingGuard<TWallTimer> timingGuard(&readerStatistics->DoReadTime);

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

        // Now all spans are not empty.
        this->ReadRows(rows + sentinelRowIndexesCount, MakeRange(readListSlice.begin(), spanEnd), dataWeight, readerStatistics);

        InsertSentinelRows(MakeRange(sentinelRowIndexes, sentinelRowIndexesCount), rows);

        return readRowCount;
    }

private:
    TMemoryHolder<ui32> ReadListHolder_;
    TMutableRange<ui32> ReadList_;

    const TSharedRange<TLegacyKey> Keys_;
    std::vector<std::unique_ptr<IColumnRefiner<TLegacyKey>>> ColumnRefiners_;

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

    void BuildReadListForWindow(TSpanMatching initialWindow) override
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

        for (size_t i = 1; i < ReadList_.size(); ++i) {
            if (ReadList_[i] == SentinelRowIndex) {
                continue;
            }

            YT_VERIFY(ReadList_[i] != ReadList_[i - 1]);
        }
    }

        ui32* BuildSentinelRowIndexes(ui32* it, ui32* end, ui32* sentinelRowIndexes)
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

    void InsertSentinelRows(TRange<ui32> sentinelRowIndexes, TMutableVersionedRow* rows)
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
    TRange<EValueType> keyTypes,
    TRange<TValueSchema> valueSchema,
    TRange<TColumnBase> columnInfos,
    TTimestamp timestamp,
    bool produceAll)
{
    return std::make_unique<TReader<ui32>>(keys, keyTypes, valueSchema, columnInfos, timestamp, produceAll);
}

std::unique_ptr<IRowsetBuilder> CreateRowsetBuilder(
    TSharedRange<TRowRange> keyRanges,
    TRange<EValueType> keyTypes,
    TRange<TValueSchema> valueSchema,
    TRange<TColumnBase> columnInfos,
    TTimestamp timestamp,
    bool produceAll)
{
    return std::make_unique<TReader<TReadSpan>>(
        keyRanges, keyTypes, valueSchema, columnInfos, timestamp, produceAll);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
