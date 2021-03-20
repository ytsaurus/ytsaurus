#include "rowset_builder.h"

namespace NYT::NNewTableClient {

using NTableClient::NullTimestamp;

ui32 TVersionedValueColumnBase::DoCollectCounts(
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

ui32 TVersionedValueColumnBase::CollectCounts(
    ui32* counts,
    TRange<TReadSpan> spans,
    ui32 position) const
{
    YT_ASSERT(!spans.Empty());
    ui32 startPosition = SkipTo(spans.Front().Lower, position);
    position = startPosition;

    for (auto [lower, upper] : spans) {
        position = DoCollectCounts(counts, lower, upper, position);
        counts += upper - lower;
    }

    return startPosition;
}

////////////////////////////////////////////////////////////////////////////////

template <EValueType Type>
class TKeyColumn
    : public TKeyColumnBase
{
public:
    TKeyColumn()
    {
        // By default column is null.
        TKeyColumnBase::InitNull();
        Value_.InitNull();
    }

    virtual void SetSegmentData(const NProto::TSegmentMeta& meta, const char* data, TTmpBuffers* tmpBuffers) override
    {
        DoReadSegment(&Value_, this, meta, data, tmpBuffers);
    }

    virtual ui32 ReadRows(
        TUnversionedValue** keys,
        TRange<TReadSpan> spans,
        ui32 position,
        ui16 id,
        TDataWeightStatistics* statistics) const override
    {
        for (auto [lower, upper] : spans) {
            position = DoReadRows(keys, lower, upper, position, id, statistics);
            keys += upper - lower;
        }
        return position;
    }

private:
    TValueExtractor<Type> Value_;

    ui32 DoReadRows(
        TUnversionedValue** keys,
        ui32 rowIndex,
        ui32 rowLimit,
        ui32 position,
        ui16 id,
        TDataWeightStatistics* statistics) const
    {
        YT_ASSERT(rowLimit <= GetSegmentRowLimit());

        position = SkipTo(rowIndex, position);

        YT_ASSERT(position < Count_);
        YT_ASSERT(rowIndex < UpperRowBound(position));

        while (rowIndex < rowLimit) {
            TUnversionedValue value;
            value.Id = id;
            value.Aggregate = false;

            YT_ASSERT(position < Count_);
            Value_.Extract(&value, position);

            ui32 nextIndex = rowLimit < UpperRowBound(position) ? rowLimit : UpperRowBound(position++);

            auto keysEnd = keys + nextIndex - rowIndex;
            rowIndex = nextIndex;

            statistics->AddFixedPart<Type>(keysEnd - keys);
            statistics->AddVariablePart<Type>(value, keysEnd - keys);

            while (keys < keysEnd) {
                (*keys++)[id] = value;
            }
        }

        return position;
    }
};


template <EValueType Type>
struct TCreateKeyColumn
{
    static std::unique_ptr<TKeyColumnBase> Do()
    {
        return std::make_unique<TKeyColumn<Type>>();
    }
};

////////////////////////////////////////////////////////////////////////////////

// TVersionedValueReader
template <EValueType Type, bool Aggregate>
class TVersionedValueColumn
    : public TVersionedValueColumnBase
{
public:
    using TVersionedValueColumnBase::SkipTo;

    explicit TVersionedValueColumn(ui16 id)
        : Id_(id)
    { }

    // Skip is allowed till SegmentRowLimit.
    virtual void SetSegmentData(const NProto::TSegmentMeta& meta, const char* data, TTmpBuffers* tmpBuffers) override
    {
        DoReadSegment(meta, data, tmpBuffers);
    }

    virtual ui32 ReadAllValues(
        TVersionedValue** values,
        const TTimestamp** timestamps,
        TRange<TReadSpan> spans,
        ui32 position,
        TDataWeightStatistics* statistics) const override
    {
        for (auto [lower, upper] : spans) {
            position = SkipTo(lower, position);
            position = DoReadAllValues(values, timestamps, lower, upper, position, statistics);
            values += upper - lower;
            timestamps += upper - lower;
        }
        return position;
    }

    virtual ui32 ReadValues(
        TRowValuesCursor* values,
        TRange<TReadSpan> spans,
        ui32 position,
        bool produceAll,
        TDataWeightStatistics* statistics) const override
    {
        for (auto [lower, upper] : spans) {
            position = SkipTo(lower, position);
            position = DoReadValues(values, lower, upper, position, produceAll, statistics);
            values += upper - lower;
        }
        return position;
    }

private:
    TVersionInfo<Aggregate> Version_;
    TValueExtractor<Type> Value_;
    const ui16 Id_;

    void DoReadSegment(const NProto::TSegmentMeta& meta, const char* data, TTmpBuffers* tmpBuffers)
    {
        // FORMAT:
        // DirectDense
        // [DiffPerRow] [TimestampIds] [IsAggregateBits] [Values] [IsNullBits]
        // DiffPerRow for each row

        // DictionaryDense
        // [DiffPerRow] [TimestampIds] [IsAggregateBits] [Values] [Ids]
        // DiffPerRow for each row

        // DirectSparse
        // [RowIndex] [TimestampIds] [IsAggregateBits] [Values] [IsNullBits]
        // RowIndex for each value

        // DictionarySparse
        // [RowIndex] [TimestampIds] [IsAggregateBits] [Values] [Ids]
        // TDictionaryString: [Ids] [Offsets] [Data]

        auto ptr = reinterpret_cast<const ui64*>(data);
        bool isDense = GetIsDense(meta, Type);

        ptr = this->Init(meta, ptr, isDense, tmpBuffers);
        ptr = Version_.Init(ptr);
        Value_.Init(meta, ptr, tmpBuffers);
    }

    ui32 DoReadAllValues(
        TVersionedValue** values,
        const TTimestamp** timestamps,
        ui32 rowIndex,
        ui32 rowLimit,
        ui32 position,
        TDataWeightStatistics* statistics) const
    {
        YT_ASSERT(rowLimit <= GetSegmentRowLimit());

        auto rowToValue = RowToValue_.GetData() + position;
        YT_ASSERT(rowIndex <= rowToValue->RowIndex);

        ui32 valueIdx = rowToValue->ValueOffset;

        while (rowToValue->RowIndex < rowLimit) {
            YT_ASSERT(rowToValue < RowToValue_.GetData() + IndexCount_);

            ui32 skipCount = rowToValue->RowIndex - rowIndex;
            values += skipCount;
            rowIndex = rowToValue->RowIndex + 1;

            ++rowToValue;
            ui32 valueIdxEnd = rowToValue->ValueOffset;

            auto* valuePtr = *values;
            YT_ASSERT(valueIdx != valueIdxEnd);

            statistics->AddFixedPart<Type>(valueIdxEnd - valueIdx);
            do {
                valuePtr->Id = Id_;
                Value_.Extract(valuePtr, valueIdx);
                Version_.Extract(valuePtr, *timestamps, valueIdx);
                statistics->AddVariablePart<Type>(*valuePtr);

                ++valuePtr;
                ++valueIdx;
            } while (valueIdx != valueIdxEnd);
            *values++ = valuePtr;
            ++timestamps;
        }

        return rowToValue - RowToValue_.GetData();
    }

    ui32 DoReadValues(
        TRowValuesCursor* values,
        ui32 rowIndex,
        ui32 rowLimit,
        ui32 position,
        bool produceAll,
        TDataWeightStatistics* statistics) const
    {
        YT_ASSERT(rowLimit <= GetSegmentRowLimit());

        auto rowToValue = RowToValue_.GetData() + position;
        YT_ASSERT(rowIndex <= rowToValue->RowIndex);

        while (rowToValue->RowIndex < rowLimit) {
            YT_ASSERT(rowToValue < RowToValue_.GetData() + IndexCount_);

            ui32 skipCount = rowToValue->RowIndex - rowIndex;
            values += skipCount;
            rowIndex = rowToValue->RowIndex + 1;

            ui32 valueIdx = rowToValue->ValueOffset;
            ++rowToValue;
            ui32 valueIdxEnd = rowToValue->ValueOffset;

            YT_ASSERT(valueIdx != valueIdxEnd);

            auto [lowerId, upperId] = values->IdRange;
            // Adjust valueIdx and valueIdxEnd according to tsIds.
            valueIdx = Version_.AdjustLowerIndex(valueIdx, valueIdxEnd, lowerId);
            valueIdxEnd = Version_.AdjustIndex(valueIdx, valueIdxEnd, upperId);

            YT_ASSERT((valueIdx == valueIdxEnd) || (lowerId != upperId));

            auto* valuePtr = values->Ptr;
            auto* timestamps = values->Timestamps;

            statistics->AddFixedPart<Type>(valueIdxEnd - valueIdx);
            while (valueIdx != valueIdxEnd) {
                valuePtr->Id = Id_;
                Value_.Extract(valuePtr, valueIdx);
                Version_.Extract(valuePtr, timestamps, valueIdx);
                statistics->AddVariablePart<Type>(*valuePtr);

                ++valuePtr;
                ++valueIdx;
                if (!produceAll && !Aggregate) {
                    break;
                }
            }
            values->Ptr = valuePtr;
            ++values;
        }

        return rowToValue - RowToValue_.GetData();
    }
};

template <EValueType Type>
struct TCreateVersionedValueColumn
{
    static std::unique_ptr<TVersionedValueColumnBase> Do(ui16 id, bool aggregate)
    {
        if (aggregate) {
            return std::make_unique<TVersionedValueColumn<Type, true>>(id);
        } else {
            return std::make_unique<TVersionedValueColumn<Type, false>>(id);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TReaderBase::TReaderBase(TRange<EValueType> keyTypes, TRange<TValueSchema> valueSchema)
{
    for (auto type : keyTypes) {
        KeyColumns_.push_back(DispatchByDataType<TCreateKeyColumn>(type));
    }

    for (auto [type, id, aggregate] : valueSchema) {
        ValueColumns_.push_back(DispatchByDataType<TCreateVersionedValueColumn>(type, id, aggregate));
    }

    Positions_.Resize(keyTypes.size() + valueSchema.size());
    memset(Positions_.GetData(), 0, sizeof(ui32) * (keyTypes.size() + valueSchema.size()));
}

void TReaderBase::ReadKeys(
    TMutableVersionedRow* rows,
    TRange<TReadSpan> spans,
    ui32 batchSize,
    TDataWeightStatistics* statistics)
{
    auto rowKeys = Allocate<TUnversionedValue*>(batchSize);

    auto rowKeyIt = rowKeys;
    for (auto rowIt = rows, rowsEnd = rows + batchSize; rowIt < rowsEnd; ++rowIt) {
        *rowKeyIt++ = rowIt->BeginKeys();
    }

    ui16 id = 0;
    for (const auto& column : GetKeyColumns()) {
        Positions_[id] = column->ReadRows(rowKeys, spans, Positions_[id], id, statistics);
        ++id;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TCompactionRowsetBuilder
    : public TVersionedRowsetBuilder
{
public:
    TCompactionRowsetBuilder(
        TRange<EValueType> keyTypes,
        TRange<TValueSchema> valueSchema)
        : TVersionedRowsetBuilder(keyTypes, valueSchema)
    { }

    virtual void ReadRows(
        TMutableVersionedRow* rows,
        TRange<TReadSpan> spans,
        TDataWeightStatistics* statistics) override
    {
        ui32 batchSize = 0;
        for (auto [lower, upper] : spans) {
            YT_VERIFY(lower != upper);
            batchSize += upper - lower;
        }

        {
            auto valueCounts = Allocate<ui32>(batchSize);
            memset(valueCounts, 0, sizeof(ui32) * batchSize);

            CollectCounts(valueCounts, spans);
            AllocateRows(rows, GetKeyColumnCount(), valueCounts, spans);
        }

        ReadKeys(rows, spans, batchSize, statistics);
        ReadValues(rows, spans, batchSize, statistics);
    }

private:
    void DoAllocateRows(
        TMutableVersionedRow* rows,
        ui32 keySize,
        const ui32* valueCounts,
        ui32 rowIndex,
        ui32 rowLimit)
    {
        auto batchSize = rowLimit - rowIndex;
        for (ui32 index = 0; index < batchSize; ++index) {
            auto [writeTimestampsIdx, writeTimestampsEnd] = GetWriteTimestampsSpan(rowIndex + index);
            auto [deleteTimestampsIdx, deleteTimestampsEnd] = GetDeleteTimestampsSpan(rowIndex + index);

            auto row = Buffer_->AllocateVersioned(
                keySize,
                valueCounts[index],
                writeTimestampsEnd - writeTimestampsIdx,
                deleteTimestampsEnd - deleteTimestampsIdx);

            std::copy(WriteTimestamps_ + writeTimestampsIdx, WriteTimestamps_ + writeTimestampsEnd, row.BeginWriteTimestamps());
            std::copy(DeleteTimestamps_ + deleteTimestampsIdx, DeleteTimestamps_ + deleteTimestampsEnd, row.BeginDeleteTimestamps());

            rows[index] = row;
        }
    }

    void AllocateRows(
        TMutableVersionedRow* rows,
        ui32 keySize,
        const ui32* valueCounts,
        TRange<TReadSpan> spans)
    {
        for (auto [lower, upper] : spans) {
            DoAllocateRows(rows, keySize, valueCounts, lower, upper);
            auto batchSize = upper - lower;
            rows += batchSize;
            valueCounts += batchSize;
        }
    }

    void ReadValues(
        TMutableVersionedRow* rows,
        TRange<TReadSpan> spans,
        ui32 batchSize,
        TDataWeightStatistics* statistics)
    {
        auto rowValues = Allocate<TVersionedValue*>(batchSize);
        auto timestamps = Allocate<const TTimestamp*>(batchSize);
        for (ui32 index = 0; index < batchSize; ++index) {
            rowValues[index] = rows[index].BeginValues();
            timestamps[index] = rows[index].BeginWriteTimestamps();
        }

        ui16 id = GetKeyColumnCount();
        for (const auto& column : GetValueColumns()) {
            Positions_[id] = column->ReadAllValues(rowValues, timestamps, spans, Positions_[id], statistics);
            ++id;
        }

        for (ui32 index = 0; index < batchSize; ++index) {
            auto valuesIt = rows[index].BeginValues();
            auto valuesEnd = rowValues[index];

            rows[index].SetValueCount(valuesEnd - valuesIt);
        }
    }
};

class TTransactionRowsetBuilder
    : public TVersionedRowsetBuilder
{
public:
    TTransactionRowsetBuilder(
        TRange<EValueType> keyTypes,
        TRange<TValueSchema> valueSchema,
        TTimestamp timestamp,
        bool produceAll)
        : TVersionedRowsetBuilder(keyTypes, valueSchema)
        , Timestamp_(timestamp)
        , ProduceAll_(produceAll)
    { }

    virtual void ReadRows(
        TMutableVersionedRow* rows,
        TRange<TReadSpan> spans,
        TDataWeightStatistics* statistics) override
    {
        ui32 batchSize = 0;
        for (auto [lower, upper] : spans) {
            YT_VERIFY(lower != upper);
            batchSize += upper - lower;
        }

        // TODO(lukyan): Build TValueDestInfo array during row allocation.
        auto tsIdRanges = Allocate<TIdRange>(batchSize);

        {
            auto valueCounts = Allocate<ui32>(batchSize);
            memset(valueCounts, 0, sizeof(ui32) * batchSize);
            CollectCounts(valueCounts, spans);

            AllocateRows(rows, GetKeyColumnCount(), valueCounts, tsIdRanges, spans);
        }

        ReadKeys(rows, spans, batchSize, statistics);
        ReadValues(rows, tsIdRanges, spans, batchSize, ProduceAll_, statistics);
    }

private:
    const TTimestamp Timestamp_;
    const bool ProduceAll_;

    void DoAllocateRows(
        TMutableVersionedRow* rows,
        ui32 keySize,
        const ui32* valueCounts,
        TIdRange* tsIdRanges,
        ui32 rowIndex,
        ui32 rowLimit)
    {
        auto batchSize = rowLimit - rowIndex;
        for (ui32 index = 0; index < batchSize; ++index) {
            auto [writeTimestampsBegin, writeTimestampsEnd] = GetWriteTimestampsSpan(rowIndex + index);
            auto [deleteTimestampsBegin, deleteTimestampsEnd] = GetDeleteTimestampsSpan(rowIndex + index);

            auto [lowerDeleteIdx, lowerWriteIdx] = GetLowerTimestampsIndexes(
                deleteTimestampsBegin,
                deleteTimestampsEnd,
                writeTimestampsBegin,
                writeTimestampsEnd,
                Timestamp_);

            // COMPAT(lukyan): Produce really all versions or all versions after last delete.
            if (ProduceAll_) {
                // Produces all versions and all delete timestamps.
                tsIdRanges[index] = std::make_pair(lowerWriteIdx - writeTimestampsBegin, writeTimestampsEnd - writeTimestampsBegin);

                auto row = Buffer_->AllocateVersioned(
                    keySize,
                    valueCounts[index],
                    writeTimestampsEnd - lowerWriteIdx,
                    deleteTimestampsEnd - lowerDeleteIdx);

                std::copy(WriteTimestamps_ + lowerWriteIdx, WriteTimestamps_ + writeTimestampsEnd, row.BeginWriteTimestamps());
                std::copy(DeleteTimestamps_ + lowerDeleteIdx, DeleteTimestamps_ + deleteTimestampsEnd, row.BeginDeleteTimestamps());

                rows[index] = row;
            } else {
                // In case of all versions produce only versions after latest (before read timestamp) delete.
                auto deleteTimestamp = lowerDeleteIdx != deleteTimestampsEnd ? DeleteTimestamps_[lowerDeleteIdx] : NullTimestamp;
                auto upperWriteIdx = GetUpperWriteIndex(lowerWriteIdx, writeTimestampsEnd, deleteTimestamp);

                tsIdRanges[index] = std::make_pair(lowerWriteIdx - writeTimestampsBegin, upperWriteIdx - writeTimestampsBegin);

                auto row = Buffer_->AllocateVersioned(
                    keySize,
                    valueCounts[index],
                    upperWriteIdx != lowerWriteIdx ? 1 : 0,
                    deleteTimestamp != NullTimestamp ? 1 : 0);

                if (lowerWriteIdx != upperWriteIdx) {
                    row.BeginWriteTimestamps()[0] = WriteTimestamps_[lowerWriteIdx];
                }

                if (deleteTimestamp != NullTimestamp) {
                    row.BeginDeleteTimestamps()[0] = deleteTimestamp;
                }

                rows[index] = row;
            }
        }
    }

    void AllocateRows(
        TMutableVersionedRow* rows,
        ui32 keySize,
        const ui32* valueCounts,
        TIdRange* tsIdRanges,
        TRange<TReadSpan> spans)
    {
        for (auto [lower, upper] : spans) {
            auto batchSize = upper - lower;
            YT_VERIFY(batchSize);

            DoAllocateRows(rows, keySize, valueCounts, tsIdRanges, lower, upper);
            rows += batchSize;
            valueCounts += batchSize;
            tsIdRanges += batchSize;
        }
    }

    std::pair<ui32, ui32> GetLowerTimestampsIndexes(
        ui32 deleteIdx,
        ui32 deleteIdxEnd,
        ui32 writeIdx,
        ui32 writeIdxEnd,
        TTimestamp readTimestamp)
    {
        // Timestamps inside row are sorted in reverse order.
        // Get delete timestamp.
        deleteIdx = LinearSearch(deleteIdx, deleteIdxEnd, [&] (auto index) {
            return DeleteTimestamps_[index] > readTimestamp;
        });

        // Get write timestamp.
        writeIdx = LinearSearch(writeIdx, writeIdxEnd, [&] (auto index) {
            return WriteTimestamps_[index] > readTimestamp;
        });

        return std::make_pair(deleteIdx, writeIdx);
    }

    ui32 GetUpperWriteIndex(
        ui32 lowerWriteIdx,
        ui32 writeIdxEnd,
        TTimestamp deleteTimestamp)
    {
        if (lowerWriteIdx == writeIdxEnd || WriteTimestamps_[lowerWriteIdx] <= deleteTimestamp) {
            return lowerWriteIdx;
        }

        // UpperWriteId is used for aggregates.
        ui32 upperWriteIdx;
        if (deleteTimestamp == NullTimestamp) {
            upperWriteIdx = writeIdxEnd;
        } else {
            // Keep only binary search?
            upperWriteIdx = BinarySearch(lowerWriteIdx + 1, writeIdxEnd, [&] (auto index) {
                return WriteTimestamps_[index] > deleteTimestamp;
            });
        }

        return upperWriteIdx;
    }

    void ReadValues(
        TMutableVersionedRow* rows,
        const TIdRange* tsIdRanges,
        TRange<TReadSpan> spans,
        ui32 batchSize,
        bool produceAll,
        TDataWeightStatistics* statistics)
    {
        auto values = Allocate<TRowValuesCursor>(batchSize);
        for (ui32 index = 0; index < batchSize; ++index) {
            values[index] = {
                rows[index].BeginValues(),
                rows[index].BeginWriteTimestamps() - tsIdRanges[index].first,
                tsIdRanges[index]};
        }

        ui16 id = GetKeyColumnCount();
        for (const auto& column : GetValueColumns()) {
            Positions_[id] = column->ReadValues(
                values,
                spans,
                Positions_[id],
                produceAll,
                statistics);
            ++id;
        }

        auto rowsEnd = rows + batchSize;
        while (rows < rowsEnd) {
            if (!rows->GetDeleteTimestampCount() && !rows->GetWriteTimestampCount()) {
                *rows = TMutableVersionedRow();
            } else {
                auto valuesIt = rows->BeginValues();
                auto valuesEnd = values->Ptr;
                rows->SetValueCount(valuesEnd - valuesIt);
            }
            ++values;
            ++rows;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TVersionedRowsetBuilder> CreateVersionedRowsetBuilder(
    TRange<EValueType> keyTypes,
    TRange<TValueSchema> valueSchema,
    TTimestamp timestamp,
    bool produceAll)
{
    if (timestamp == NTransactionClient::AllCommittedTimestamp) {
        return std::make_unique<TCompactionRowsetBuilder>(keyTypes, valueSchema);
    } else {
        return std::make_unique<TTransactionRowsetBuilder>(keyTypes, valueSchema, timestamp, produceAll);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
