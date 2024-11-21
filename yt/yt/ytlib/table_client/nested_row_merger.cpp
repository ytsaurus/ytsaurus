#include "nested_row_merger.h"
#include "config.h"

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/unversioned_value.h>
#include <yt/yt/client/table_client/versioned_row.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/private.h>

#include <yt/yt/core/misc/heap.h>

#include <yt/yt/library/numeric/algorithm_helpers.h>

namespace NYT::NTableClient {

using namespace NTransactionClient;
using namespace NTabletClient;
using namespace NYTree;

using NYson::EYsonItemType;
using NYson::EYsonType;
using NYson::TYsonPullParser;
using NYson::TYsonPullParserCursor;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = TableClientLogger;

////////////////////////////////////////////////////////////////////////////////

void AggregateReplace(TUnversionedValue* state, const TUnversionedValue& value)
{
    *state = value;
}

void AggregateSumInt64(TUnversionedValue* state, const TUnversionedValue& value)
{
    if (state->Type == EValueType::Null) {
        *state = value;
    } else if (value.Type != EValueType::Null) {
        state->Data.Int64 += value.Data.Int64;
    }
}

void AggregateSumUint64(TUnversionedValue* state, const TUnversionedValue& value)
{
    if (state->Type == EValueType::Null) {
        *state = value;
    } else if (value.Type != EValueType::Null) {
        state->Data.Uint64 += value.Data.Uint64;
    }
}

void AggregateSumDouble(TUnversionedValue* state, const TUnversionedValue& value)
{
    if (state->Type == EValueType::Null) {
        *state = value;
    } else if (value.Type != EValueType::Null) {
        state->Data.Double += value.Data.Double;
    }
}

void AggregateMaxInt64(TUnversionedValue* state, const TUnversionedValue& value)
{
    if (state->Type == EValueType::Null) {
        *state = value;
    } else if (value.Type != EValueType::Null) {
        state->Data.Int64 = std::max(state->Data.Int64, value.Data.Int64);
    }
}

void AggregateMaxUint64(TUnversionedValue* state, const TUnversionedValue& value)
{
    if (state->Type == EValueType::Null) {
        *state = value;
    } else if (value.Type != EValueType::Null) {
        state->Data.Uint64 = std::max(state->Data.Uint64, value.Data.Uint64);
    }
}

void AggregateMaxDouble(TUnversionedValue* state, const TUnversionedValue& value)
{
    if (state->Type == EValueType::Null) {
        *state = value;
    } else if (value.Type != EValueType::Null) {
        state->Data.Double = std::max(state->Data.Double, value.Data.Double);
    }
}

TNestedColumnsSchema GetNestedColumnsSchema(TTableSchemaPtr tableSchema)
{
    std::vector<TNestedKeyColumn> keyColumns;
    std::vector<TNestedValueColumn> valueColumns;

    // TODO(lukyan): Support multiple nested tables.
    TStringBuf nestedTableName;

    for (const auto& column : tableSchema->Columns()) {
        const auto& aggregate = column.Aggregate();

        if (!aggregate) {
            continue;
        }

        auto nestedColumn = TryParseNestedAggregate(*aggregate);
        if (!nestedColumn) {
            continue;
        }

        if (nestedTableName.empty()) {
            nestedTableName = nestedColumn->NestedTableName;
        } else if (nestedTableName != nestedColumn->NestedTableName) {
            YT_LOG_ALERT("Multiple nested tables are not supported yet (FirstName: %v, SecondName: %v, ColumnName: %v)",
                nestedTableName,
                nestedColumn->NestedTableName,
                column.Name());

            THROW_ERROR_EXCEPTION("Multiple nested tables are not supported yet")
                << TErrorAttribute("first_name", nestedTableName)
                << TErrorAttribute("second_name", nestedColumn->NestedTableName)
                << TErrorAttribute("column_name", column.Name());
        }

        auto elementType = GetNestedColumnElementType(column.LogicalType().Get());

        if (nestedColumn->IsKey) {
            keyColumns.push_back({static_cast<ui16>(tableSchema->GetColumnIndex(column)), elementType});
        } else {
            TAggregateFunction* aggregateFunction = AggregateReplace;

            if (!nestedColumn->Aggregate.empty()) {
                if (nestedColumn->Aggregate == "sum") {
                    if (elementType == EValueType::Int64) {
                        aggregateFunction = &AggregateSumInt64;
                    } else if (elementType == EValueType::Uint64) {
                        aggregateFunction = &AggregateSumUint64;
                    } else if (elementType == EValueType::Double) {
                        aggregateFunction = &AggregateSumDouble;
                    } else {
                        THROW_ERROR_EXCEPTION("Unsupported nested elemnt type")
                            << TErrorAttribute("type", elementType);
                    }
                } else if (nestedColumn->Aggregate == "max") {
                    if (elementType == EValueType::Int64) {
                        aggregateFunction = &AggregateMaxInt64;
                    } else if (elementType == EValueType::Uint64) {
                        aggregateFunction = &AggregateMaxUint64;
                    } else if (elementType == EValueType::Double) {
                        aggregateFunction = &AggregateMaxDouble;
                    } else {
                        THROW_ERROR_EXCEPTION("Unsupported nested elemnt type")
                            << TErrorAttribute("type", elementType);
                    }
                } else {
                    THROW_ERROR_EXCEPTION("Unsupported aggregate function for nested column")
                        << TErrorAttribute("aggregate_function", nestedColumn->Aggregate);
                }
            }

            valueColumns.push_back({
                static_cast<ui16>(tableSchema->GetColumnIndex(column)),
                elementType,
                aggregateFunction});
        }
    }

    return {keyColumns, valueColumns};
}

const TNestedKeyColumn* GetNestedColumnById(TRange<TNestedKeyColumn> keyColumns, ui16 columnId)
{
    auto foundIt = BinarySearch(keyColumns.begin(), keyColumns.end(), [&] (const auto* it) {
        return it->Id < columnId;
    });

    if (foundIt != keyColumns.end() && foundIt->Id == columnId) {
        return foundIt;
    }

    return nullptr;
}

const TNestedValueColumn* GetNestedColumnById(TRange<TNestedValueColumn> keyColumns, ui16 columnId)
{
    auto foundIt = BinarySearch(keyColumns.begin(), keyColumns.end(), [&] (const auto* it) {
        return it->Id < columnId;
    });

    if (foundIt != keyColumns.end() && foundIt->Id == columnId) {
        return foundIt;
    }

    return nullptr;
}

int UnpackNestedValuesList(std::vector<TUnversionedValue>* parsedValues, TStringBuf data, EValueType listItemType)
{
    try {
        TMemoryInput memoryInput(data);
        auto parser = TYsonPullParser(&memoryInput, EYsonType::Node);
        auto cursor = TYsonPullParserCursor(&parser);

        TUnversionedValue parsedValue;

        int itemCount = 0;

        auto validateType = [] (EValueType expected, EValueType actual, const auto& value) {
            if (expected != actual) {
                THROW_ERROR_EXCEPTION("Type mismatch")
                    << TErrorAttribute("expected", expected)
                    << TErrorAttribute("actual", actual)
                    << TErrorAttribute("value", value);
            }
        };

        cursor.ParseList([&] (TYsonPullParserCursor* cursor) {
            auto currentType = cursor->GetCurrent().GetType();
            switch (currentType) {
                case EYsonItemType::EntityValue: {
                    parsedValue = MakeUnversionedNullValue();
                    break;
                }
                case EYsonItemType::Int64Value: {
                    auto value = cursor->GetCurrent().UncheckedAsInt64();
                    validateType(listItemType, EValueType::Int64, value);
                    parsedValue = MakeUnversionedInt64Value(value);
                    break;
                }
                case EYsonItemType::Uint64Value: {
                    auto value = cursor->GetCurrent().UncheckedAsUint64();
                    validateType(listItemType, EValueType::Uint64, value);
                    parsedValue = MakeUnversionedUint64Value(value);
                    break;
                }
                case EYsonItemType::DoubleValue: {
                    auto value = cursor->GetCurrent().UncheckedAsDouble();
                    validateType(listItemType, EValueType::Double, value);
                    parsedValue = MakeUnversionedDoubleValue(value);
                    break;
                }
                case EYsonItemType::StringValue: {
                    auto value = cursor->GetCurrent().UncheckedAsString();
                    validateType(listItemType, EValueType::String, value);
                    parsedValue = MakeUnversionedStringValue(value);
                    break;
                }
                default:
                    THROW_ERROR_EXCEPTION("Unexpected value")
                        << TErrorAttribute("type", currentType);
            }

            if (parsedValues) {
                parsedValues->push_back(parsedValue);
            }
            ++itemCount;

            cursor->Next();
        });

        return itemCount;
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error unpacking nested values list")
            << TErrorAttribute("data", data)
            << ex;
    }
}

// Build yson list from values.
TUnversionedValue PackValues(TRange<TUnversionedValue> values, TRowBuffer* rowBuffer)
{
    TString resultYson;
    TStringOutput output(resultYson);
    NYson::TYsonWriter writer(&output);

    writer.OnBeginList();
    for (int index = 0; index < std::ssize(values); ++index) {
        const auto& valueArg = values[index];

        writer.OnListItem();

        switch (valueArg.Type) {
            case EValueType::Int64:
                writer.OnInt64Scalar(valueArg.Data.Int64);
                break;
            case EValueType::Uint64:
                writer.OnUint64Scalar(valueArg.Data.Uint64);
                break;
            case EValueType::Double:
                writer.OnDoubleScalar(valueArg.Data.Double);
                break;
            case EValueType::Boolean:
                writer.OnBooleanScalar(valueArg.Data.Boolean);
                break;
            case EValueType::String:
                writer.OnStringScalar(valueArg.AsStringBuf());
                break;
            case EValueType::Any:
                writer.OnRaw(valueArg.AsStringBuf());
                break;
            case EValueType::Null:
                writer.OnEntity();
                break;
            default:
                THROW_ERROR_EXCEPTION("Unexpected type %Qlv of value #%v",
                    valueArg.Type,
                    index);
        }
    }
    writer.OnEndList();

    return rowBuffer->CaptureValue(MakeUnversionedCompositeValue(resultYson));
}

void TNestedTableMerger::UnpackKeyColumn(
    ui16 keyColumnId,
    int mergeStreamCount,
    TRange<TVersionedValue> keyColumn,
    TNestedKeyColumn keyColumnSchema)
{
    // Expect each nested column has equal stream count.
    if (std::ssize(keyColumn) != mergeStreamCount) {
        YT_LOG_ALERT("Merge stream count mismatch (ColumnId: %v, Expected: %v, Actual: %v)",
            keyColumnId,
            mergeStreamCount,
            std::ssize(keyColumn));

        THROW_ERROR_EXCEPTION("Merge stream count mismatch")
            << TErrorAttribute("column_id", keyColumnId)
            << TErrorAttribute("expected", mergeStreamCount)
            << TErrorAttribute("actual", std::ssize(keyColumn));
    }

    auto& unpackedColumn = UnpackedKeys_[keyColumnId];

    for (int index = 0; index < mergeStreamCount; ++index) {
        auto timestamp = keyColumn[index].Timestamp;

        if (keyColumn[index].Type != EValueType::Null) {
            UnpackNestedValuesList(
                &unpackedColumn,
                keyColumn[index].AsStringBuf(),
                keyColumnSchema.Type);
        }

        if (keyColumnId == 0) {
            YT_ASSERT(Timestamps_.empty() || timestamp >= Timestamps_.back());
            Timestamps_.push_back(timestamp);
            Offsets_.push_back(unpackedColumn.size());
        } else {
            if (Timestamps_[index] != timestamp) {
                THROW_ERROR_EXCEPTION("Timestamp mismatch in nested key columns")
                    << TErrorAttribute("column_id", keyColumnId)
                    << TErrorAttribute("expected", Timestamps_[index])
                    << TErrorAttribute("actual", timestamp);
            }

            if (Offsets_[index + 1] != std::ssize(unpackedColumn)) {
                THROW_ERROR_EXCEPTION("Mismatch item count in nested key columns")
                    << TErrorAttribute("column_id", keyColumnId)
                    << TErrorAttribute("expected", Offsets_[index + 1])
                    << TErrorAttribute("actual", std::ssize(unpackedColumn));
            }
        }
    }
}

void TNestedTableMerger::Reset(int keyWidth, int mergeStreamCount)
{
    Timestamps_.clear();
    UnpackedKeys_.clear();
    Offsets_ = {0};

    Timestamps_.reserve(mergeStreamCount);
    UnpackedKeys_.resize(keyWidth);
}

void TNestedTableMerger::UnpackKeyColumns(
    TRange<TMutableRange<TVersionedValue>> keyColumns,
    TRange<TNestedKeyColumn> keyColumnsSchema)
{
    if (keyColumns.Empty()) {
        return;
    }

    int keyWidth = std::ssize(keyColumns);
    auto mergeStreamCount = std::ssize(keyColumns.Front());

    Reset(keyWidth, mergeStreamCount);

    for (ui16 keyColumnId = 0; keyColumnId < keyWidth; ++keyColumnId) {
        UnpackKeyColumn(keyColumnId, mergeStreamCount, keyColumns[keyColumnId], keyColumnsSchema[keyColumnId]);
    }

    OrderingTranslationLayer_.resize(Offsets_.back());
    std::iota(OrderingTranslationLayer_.begin(), OrderingTranslationLayer_.end(), 0);

    BuildMergeScript();
}

void TNestedTableMerger::UnpackKeyColumns(
    TRange<std::vector<TVersionedValue>> keyColumns,
    TRange<TNestedKeyColumn> keyColumnsSchema)
{
    if (keyColumns.Empty()) {
        return;
    }

    int keyWidth = std::ssize(keyColumns);
    auto mergeStreamCount = std::ssize(keyColumns.Front());

    Reset(keyWidth, mergeStreamCount);

    for (ui16 keyColumnId = 0; keyColumnId < keyWidth; ++keyColumnId) {
        UnpackKeyColumn(keyColumnId, mergeStreamCount, keyColumns[keyColumnId], keyColumnsSchema[keyColumnId]);
    }

    OrderingTranslationLayer_.resize(Offsets_.back());
    std::iota(OrderingTranslationLayer_.begin(), OrderingTranslationLayer_.end(), 0);

    for (int rowIndex = 0; rowIndex < mergeStreamCount; ++rowIndex) {
        int from = Offsets_[rowIndex];
        int to = Offsets_[rowIndex + 1];

        for (int keyColumnId = keyWidth - 1; keyColumnId >= 0; --keyColumnId) {
            auto& unpackedColumn = UnpackedKeys_[keyColumnId];
            YT_ASSERT(OrderingTranslationLayer_.size() == unpackedColumn.size());

            std::stable_sort(
                OrderingTranslationLayer_.begin() + from,
                OrderingTranslationLayer_.begin() + to,
                [&] (int lhs, int rhs) { return unpackedColumn[lhs] < unpackedColumn[rhs]; });
        }
    }

    BuildMergeScript();
}

void TNestedTableMerger::BuildMergeScript()
{
    RowIds_.clear();
    NestedRowCounts_.clear();

    if (Offsets_.size() == 1) {
        return;
    }

    int keyWidth = std::ssize(UnpackedKeys_);

    // Make heap to merge values.

    CurrentOffsets_ = Offsets_;

    auto compareKey = [&] (int lhsIndex, int rhsIndex) {
        lhsIndex = OrderingTranslationLayer_[lhsIndex];
        rhsIndex = OrderingTranslationLayer_[rhsIndex];
        for (ui16 keyColumnId = 0; keyColumnId < keyWidth; ++keyColumnId) {
            if (UnpackedKeys_[keyColumnId][lhsIndex] != UnpackedKeys_[keyColumnId][rhsIndex]) {
                return UnpackedKeys_[keyColumnId][lhsIndex] < UnpackedKeys_[keyColumnId][rhsIndex] ? -1 : 1;
            }
        }

        return 0;
    };

    // Returns true once stream is depleted.
    auto advanceStream = [&] (int id) {
        auto endOffset = Offsets_[id + 1];
        YT_VERIFY(CurrentOffsets_[id] != endOffset);

        return ++CurrentOffsets_[id] == endOffset;
    };

    RowIdHeap_.clear();

    for (int id = 0; id < std::ssize(CurrentOffsets_) - 1; ++id) {
        if (CurrentOffsets_[id] != Offsets_[id + 1]) {
            RowIdHeap_.push_back(id);
        }
    }

    if (RowIdHeap_.empty()) {
        return;
    }

    NYT::MakeHeap(RowIdHeap_.begin(), RowIdHeap_.end(), [&] (int lhs, int rhs) {
        auto cmpResult = compareKey(CurrentOffsets_[lhs], CurrentOffsets_[rhs]);
        return cmpResult == 0 ? lhs < rhs : cmpResult < 0;
    });

    size_t savedIdsSize = RowIds_.size();

    int topId = RowIdHeap_.front();
    auto currentItem = CurrentOffsets_[topId];

    while (true) {
        RowIds_.push_back(topId);
        if (Y_UNLIKELY(advanceStream(topId))) {
            RowIdHeap_.front() = RowIdHeap_.back();
            RowIdHeap_.pop_back();
            if (RowIdHeap_.empty()) {
                NestedRowCounts_.push_back(RowIds_.size() - savedIdsSize);
                savedIdsSize = RowIds_.size();
                break;
            }
        }

        // TODO(lukyan): Can return if top changes.
        SiftDown(RowIdHeap_.begin(), RowIdHeap_.end(), RowIdHeap_.begin(), [&] (int lhs, int rhs) {
            auto cmpResult = compareKey(CurrentOffsets_[lhs], CurrentOffsets_[rhs]);
            return cmpResult == 0 ? lhs < rhs : cmpResult < 0;
        });

        topId = RowIdHeap_.front();
        if (compareKey(currentItem, CurrentOffsets_[topId]) != 0) {
            NestedRowCounts_.push_back(RowIds_.size() - savedIdsSize);
            savedIdsSize = RowIds_.size();
            currentItem = CurrentOffsets_[topId];
        }
    }
}

TUnversionedValue TNestedTableMerger::BuildMergedKeyColumns(
    TRange<int> counts,
    TRange<int> rowIds,
    TRange<TUnversionedValue> unpackedKeys,
    TRowBuffer* rowBuffer)
{
    CurrentOffsets_ = Offsets_;

    const auto* idPtr = rowIds.Begin();
    ResultValues_.clear();
    for (auto count : counts) {
        // Ids and timestamps are in increasing order.

        auto id = *idPtr++;
        ResultValues_.push_back(unpackedKeys[OrderingTranslationLayer_[CurrentOffsets_[id]++]]);

        for (int i = 1; i < count; ++i) {
            ++CurrentOffsets_[*idPtr++];
        }
    }

    return PackValues(ResultValues_, rowBuffer);
}

TVersionedValue TNestedTableMerger::BuildMergedValueColumn(
    TRange<int> counts,
    TRange<int> rowIds,
    TRange<TTimestamp> timestamps,
    TRange<TVersionedValue> values,
    EValueType elementType,
    TAggregateFunction* aggregateFunction,
    TRowBuffer* rowBuffer)
{
    // Unpack values.
    UnpackedValues_.clear();
    CurrentOffsets_.assign(timestamps.size(), -1);

    YT_VERIFY(values.Size() <= timestamps.Size());

    for (const auto& value : values) {
        auto timestampIt = LowerBound(timestamps.Begin(), timestamps.End(), value.Timestamp);

        if (timestampIt == timestamps.End() && *timestampIt != value.Timestamp) {
            THROW_ERROR_EXCEPTION("Cannot find matching timestamp for value column")
                << TErrorAttribute("timestamp", value.Timestamp);
        }

        if (value.Type == EValueType::Null) {
            continue;
        }

        int id = timestampIt - timestamps.Begin();

        CurrentOffsets_[id] = UnpackedValues_.size();

        // TODO(lukyan): Set aggregate flag from initial versioned value.
        UnpackNestedValuesList(&UnpackedValues_, value.AsStringBuf(), elementType);
    }

    const auto* rowIdPtr = rowIds.Begin();

    ResultValues_.clear();
    for (auto count : counts) {
        // Ids and timestamps in increasing order.

        auto rowId = *rowIdPtr++;

        TUnversionedValue mergeState = MakeUnversionedNullValue();

        // Do not merge if CurrentOffsets_[id] is -1 (npos).
        if (CurrentOffsets_[rowId] != -1) {
            mergeState = UnpackedValues_[OrderingTranslationLayer_[CurrentOffsets_[rowId]++]];
        }

        for (int index = 1; index < count; ++index) {
            rowId = *rowIdPtr++;
            auto value = MakeUnversionedNullValue();
            // Do not merge if CurrentOffsets_[id] is -1 (npos).
            if (CurrentOffsets_[rowId] != -1) {
                value = UnpackedValues_[OrderingTranslationLayer_[CurrentOffsets_[rowId]++]];
            }
            (*aggregateFunction)(&mergeState, value);
        }

        ResultValues_.push_back(mergeState);
    }

    TVersionedValue result;
    static_cast<TUnversionedValue&>(result) = PackValues(ResultValues_, rowBuffer);
    result.Timestamp = timestamps.Back();
    return result;
}

TVersionedValue TNestedTableMerger::BuildMergedKeyColumns(int index, TRowBuffer* rowBuffer)
{
    TVersionedValue state;
    static_cast<TUnversionedValue&>(state) = BuildMergedKeyColumns(NestedRowCounts_, RowIds_, UnpackedKeys_[index], rowBuffer);
    state.Timestamp = Timestamps_.back();
    return state;
}

TVersionedValue TNestedTableMerger::BuildMergedValueColumn(
    TRange<TVersionedValue> values,
    EValueType elementType,
    TAggregateFunction* aggregateFunction,
    TRowBuffer* rowBuffer)
{
    return BuildMergedValueColumn(NestedRowCounts_, RowIds_, Timestamps_, values, elementType, aggregateFunction, rowBuffer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
