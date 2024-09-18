#include "nested_row_merger.h"
#include "config.h"
#include "private.h"

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/unversioned_value.h>
#include <yt/yt/client/table_client/versioned_row.h>
#include <yt/yt/client/table_client/schema.h>

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
            auto Logger = TableClientLogger;
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
                        aggregateFunction = &AggregateSumInt64;
                    } else if (elementType == EValueType::Double) {
                        aggregateFunction = &AggregateSumInt64;
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

TUnversionedValue PackValues(TRange<TUnversionedValue> values, TRowBuffer* rowBuffer)
{
    // Build yson list from values.

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
        auto Logger = TableClientLogger;
        YT_LOG_ALERT("Merge stream count mismatch (ColumnId: %v, Expected: %v, Actual: %v)",
            keyColumnId,
            mergeStreamCount,
            std::ssize(keyColumn));

        THROW_ERROR_EXCEPTION("Merge stream count mismatch")
            << TErrorAttribute("column_id", keyColumnId)
            << TErrorAttribute("expected", mergeStreamCount)
            << TErrorAttribute("actual", std::ssize(keyColumn));
    }

    YT_VERIFY(std::ssize(keyColumn) == mergeStreamCount);

    for (int index = 0; index < mergeStreamCount; ++index) {
        auto timestamp = keyColumn[index].Timestamp;

        if (keyColumnId == 0) {
            Timestamps_.push_back(timestamp);
            Offsets_.push_back(UnpackedKeys_[keyColumnId].size());
        } else {
            if (Timestamps_[index] != timestamp) {
                THROW_ERROR_EXCEPTION("Timestamp mismatch in nested key columns")
                    << TErrorAttribute("column_id", keyColumnId)
                    << TErrorAttribute("expected", Timestamps_[index])
                    << TErrorAttribute("actual", timestamp);
            }

            if (Offsets_[index] != std::ssize(UnpackedKeys_[keyColumnId])) {
                THROW_ERROR_EXCEPTION("Mismatch item count in nested key columns")
                    << TErrorAttribute("column_id", keyColumnId)
                    << TErrorAttribute("expected", Offsets_[index])
                    << TErrorAttribute("actual", std::ssize(UnpackedKeys_[keyColumnId]));
            }
        }

        if (keyColumn[index].Type == EValueType::Null) {
            continue;
        }

        UnpackNestedValuesList(
            &UnpackedKeys_[keyColumnId],
            keyColumn[index].AsStringBuf(),
            keyColumnSchema.Type);
    }
}

void TNestedTableMerger::Reset(int keyWidth, int mergeStreamCount)
{
    Timestamps_.clear();
    UnpackedKeys_.clear();
    Offsets_.clear();

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
}

void TNestedTableMerger::BuildMergeScript()
{
    Ids_.clear();
    Counts_.clear();

    if (Offsets_.empty()) {
        return;
    }

    int keyWidth = std::ssize(UnpackedKeys_);

    // Make heap to merge values.

    CurrentOffsets_ = Offsets_;
    EndOffsets_.resize(CurrentOffsets_.size());

    for (int index = 1; index < std::ssize(CurrentOffsets_); ++index) {
        EndOffsets_[index - 1] = CurrentOffsets_[index];
    }
    EndOffsets_.back() = UnpackedKeys_.front().size();

    auto compareKey = [&] (ui32 lhsIndex, ui32 rhsIndex) -> int {
        for (ui16 keyColumnId = 0; keyColumnId < keyWidth; ++keyColumnId) {
            if (UnpackedKeys_[keyColumnId][lhsIndex] != UnpackedKeys_[keyColumnId][rhsIndex]) {
                return UnpackedKeys_[keyColumnId][lhsIndex] < UnpackedKeys_[keyColumnId][rhsIndex] ? -1 : 1;
            }
        }

        return 0;
    };

    auto skipStream = [&] (int id) {
        auto endOffset = EndOffsets_[id];
        YT_VERIFY(CurrentOffsets_[id] != endOffset);

        return ++CurrentOffsets_[id] == endOffset;
    };

    Heap_.clear();

    for (int id = 0; id < std::ssize(CurrentOffsets_); ++id) {
        if (CurrentOffsets_[id] != EndOffsets_[id]) {
            Heap_.push_back(id);
        }
    }

    if (Heap_.empty()) {
        return;
    }

    NYT::MakeHeap(Heap_.begin(), Heap_.end(), [&] (int lhs, int rhs) {
        auto cmpResult = compareKey(CurrentOffsets_[lhs], CurrentOffsets_[rhs]);
        return cmpResult == 0 ? lhs < rhs : cmpResult < 0;
    });

    size_t savedIdsSize = Ids_.size();

    int topId = Heap_.front();
    auto currentItem = CurrentOffsets_[topId];

    while (true) {
        Ids_.push_back(topId);
        if (Y_UNLIKELY(skipStream(topId))) {
            Heap_.front() = Heap_.back();
            Heap_.pop_back();
            if (Heap_.empty()) {
                Counts_.push_back(Ids_.size() - savedIdsSize);
                savedIdsSize = Ids_.size();
                break;
            }
        }

        // TODO(lukyan): Can return if top changes.
        SiftDown(Heap_.begin(), Heap_.end(), Heap_.begin(), [&] (int lhs, int rhs) {
            auto cmpResult = compareKey(CurrentOffsets_[lhs], CurrentOffsets_[rhs]);
            return cmpResult == 0 ? lhs < rhs : cmpResult < 0;
        });

        topId = Heap_.front();
        // TODO(lukyan): If topId is same no need to compare keys. They are not equal.
        if (compareKey(currentItem, CurrentOffsets_[topId]) != 0) {
            Counts_.push_back(Ids_.size() - savedIdsSize);
            savedIdsSize = Ids_.size();
            currentItem = CurrentOffsets_[topId];
        }
    }
}

TUnversionedValue TNestedTableMerger::BuildMergedKeyColumns(
    TRange<int> counts,
    TRange<int> ids,
    TRange<TUnversionedValue> unpackedKeys,
    TRowBuffer* rowBuffer)
{
    CurrentOffsets_ = Offsets_;

    const auto* idPtr = ids.Begin();
    ResultValues_.clear();
    for (auto count : counts) {
        // Ids and timestamps are in increasing order.

        auto id = *idPtr++;
        ResultValues_.push_back(unpackedKeys[CurrentOffsets_[id]++]);

        for (int i = 1; i < count; ++i) {
            ++CurrentOffsets_[*idPtr++];
        }
    }

    return PackValues(ResultValues_, rowBuffer);
}

TVersionedValue TNestedTableMerger::ApplyMergeScript(
    TRange<int> counts,
    TRange<int> ids,
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

    const auto* idPtr = ids.Begin();

    ResultValues_.clear();
    for (auto count : counts) {
        // Ids and timestamps in increasing order.

        auto id = *idPtr++;

        TUnversionedValue mergeState = MakeUnversionedNullValue();

        // Do not merge if CurrentOffsets_[id] is -1 (npos).
        if (CurrentOffsets_[id] != -1) {
            mergeState = UnpackedValues_[CurrentOffsets_[id]++];
        }

        for (int index = 1; index < count; ++index) {
            auto id = *idPtr++;
            auto value = MakeUnversionedNullValue();
            // Do not merge if CurrentOffsets_[id] is -1 (npos).
            if (CurrentOffsets_[id] != -1) {
                value = UnpackedValues_[CurrentOffsets_[id]++];
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
    static_cast<TUnversionedValue&>(state) = BuildMergedKeyColumns(Counts_, Ids_, UnpackedKeys_[index], rowBuffer);
    state.Timestamp = Timestamps_.back();
    return state;
}

TVersionedValue TNestedTableMerger::ApplyMergeScript(
    TRange<TVersionedValue> values,
    EValueType elementType,
    TAggregateFunction* aggregateFunction,
    TRowBuffer* rowBuffer)
{
    return ApplyMergeScript(Counts_, Ids_, Timestamps_, values, elementType, aggregateFunction, rowBuffer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
