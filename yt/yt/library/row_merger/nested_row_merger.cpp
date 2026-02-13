#include "nested_row_merger.h"

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/unversioned_value.h>
#include <yt/yt/client/table_client/versioned_row.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/private.h>
#include <yt/yt/client/table_client/lightweight_yson_list_parser.h>

#include <yt/yt/core/misc/heap.h>

#include <yt/yt/library/numeric/algorithm_helpers.h>

namespace NYT::NRowMerger {

using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYTree;

using NYson::EYsonItemType;
using NYson::EYsonType;
using NYson::TYsonPullParser;
using NYson::TYsonPullParserCursor;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TableClientLogger;

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

void AggregateMaxString(TUnversionedValue* state, const TUnversionedValue& value)
{
    if (state->Type == EValueType::Null) {
        *state = value;
    } else if (value.Type != EValueType::Null) {
        if (value.AsStringBuf() > state->AsStringBuf()) {
            *state = value;
        }
    }
}

TAggregateFunction* GetSimpleAggregateFunction(TStringBuf name, EValueType type)
{
    if (name == "sum") {
        if (type == EValueType::Int64) {
            return &AggregateSumInt64;
        } else if (type == EValueType::Uint64) {
            return &AggregateSumUint64;
        } else if (type == EValueType::Double) {
            return &AggregateSumDouble;
        } else {
            THROW_ERROR_EXCEPTION("Unsupported simple aggregate for type")
                << TErrorAttribute("type", type);
        }
    } else if (name == "max") {
        if (type == EValueType::Int64) {
            return &AggregateMaxInt64;
        } else if (type == EValueType::Uint64) {
            return &AggregateMaxUint64;
        } else if (type == EValueType::Double) {
            return &AggregateMaxDouble;
        } else if (type == EValueType::String) {
            return &AggregateMaxString;
        } else {
            THROW_ERROR_EXCEPTION("Unsupported simple aggregate for type")
                << TErrorAttribute("type", type);
        }
    } else {
        THROW_ERROR_EXCEPTION("Unsupported simple aggregate")
            << TErrorAttribute("aggregate_function", name);
    }
}

TNestedColumnsSchema GetNestedColumnsSchema(const TTableSchema& tableSchema)
{
    std::vector<TNestedKeyColumn> keyColumns;
    std::vector<TNestedValueColumn> valueColumns;

    // TODO(lukyan): Support multiple nested tables.
    TStringBuf nestedTableName;

    for (const auto& column : tableSchema.Columns()) {
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
            keyColumns.push_back({static_cast<ui16>(tableSchema.GetColumnIndex(column)), elementType});
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
                        THROW_ERROR_EXCEPTION("Unsupported nested element type")
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
                        THROW_ERROR_EXCEPTION("Unsupported nested element type")
                            << TErrorAttribute("type", elementType);
                    }
                } else {
                    THROW_ERROR_EXCEPTION("Unsupported aggregate function for nested column")
                        << TErrorAttribute("aggregate_function", nestedColumn->Aggregate);
                }
            }

            valueColumns.push_back({
                static_cast<ui16>(tableSchema.GetColumnIndex(column)),
                elementType,
                aggregateFunction});
        }
    }

    return {keyColumns, valueColumns};
}

TNestedColumnsSchema FilterNestedColumnsSchema(const TNestedColumnsSchema& nestedSchema, TRange<int> columnIds)
{
    bool hasNestedColumns = false;
    for (auto id : columnIds) {
        if (FindNestedColumnById(nestedSchema.KeyColumns, id)) {
            hasNestedColumns = true;
        }

        if (FindNestedColumnById(nestedSchema.ValueColumns, id)) {
            hasNestedColumns = true;
        }
    }

    if (!hasNestedColumns) {
        return {};
    }

    TNestedColumnsSchema result;
    result.KeyColumns = nestedSchema.KeyColumns;

    for (const auto& valueColumn : nestedSchema.ValueColumns) {
        if (std::find(columnIds.begin(), columnIds.end(), valueColumn.Id) != columnIds.end()) {
            result.ValueColumns.push_back(valueColumn);
        }
    }

    return result;
}

const TNestedKeyColumn* FindNestedColumnById(TRange<TNestedKeyColumn> keyColumns, ui16 columnId)
{
    auto foundIt = BinarySearch(keyColumns.begin(), keyColumns.end(), [&] (const auto* it) {
        return it->Id < columnId;
    });

    if (foundIt != keyColumns.end() && foundIt->Id == columnId) {
        return foundIt;
    }

    return nullptr;
}

const TNestedValueColumn* FindNestedColumnById(TRange<TNestedValueColumn> keyColumns, ui16 columnId)
{
    auto foundIt = BinarySearch(keyColumns.begin(), keyColumns.end(), [&] (const auto* it) {
        return it->Id < columnId;
    });

    if (foundIt != keyColumns.end() && foundIt->Id == columnId) {
        return foundIt;
    }

    return nullptr;
}

int UnpackNestedValuesListFast(std::vector<TUnversionedValue>* parsedValues, TStringBuf data, EValueType listItemType)
{
    int itemCount = 0;
    DoUnpackValuesTyped(
        [&] (const TUnversionedValue& value) {
            if (parsedValues) {
                parsedValues->push_back(value);
            }
            ++itemCount;
            return false;
        },
        data,
        listItemType);

    return itemCount;
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

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE ui32 TSimpleOutputBuffer::RemainingBytes() const
{
    ui32 currentSize = CurrentPtr_ - Data_.get();
    return Capacity_ - currentSize;
}

Y_FORCE_INLINE char* TSimpleOutputBuffer::Current() const
{
    return CurrentPtr_;
}

Y_FORCE_INLINE void TSimpleOutputBuffer::Advance(ui32 count)
{
    CurrentPtr_ += count;
}

Y_FORCE_INLINE void TSimpleOutputBuffer::PushBack(char ch)
{
    Reserve(1);
    *CurrentPtr_++ = ch;
}

Y_FORCE_INLINE void TSimpleOutputBuffer::Write(const void* data, ui32 length)
{
    Reserve(length);
    ::memcpy(CurrentPtr_, data, length);
    Advance(length);
}

Y_FORCE_INLINE void TSimpleOutputBuffer::Clear()
{
    CurrentPtr_ = Data_.get();
}

Y_FORCE_INLINE void TSimpleOutputBuffer::Reserve(ui32 count)
{
    ui32 currentSize = CurrentPtr_ - Data_.get();
    if (currentSize + count <= Capacity_) {
        return;
    }

    auto newCapacity = FastClp2(currentSize + count);
    auto newData = std::make_unique<char[]>(newCapacity);
    ::memcpy(newData.get(), Data_.get(), currentSize);
    Data_ = std::move(newData);
    Capacity_ = newCapacity;
    CurrentPtr_ = Data_.get() + currentSize;
}

Y_FORCE_INLINE TStringBuf TSimpleOutputBuffer::GetBuffer() const
{
    return {Data_.get(), CurrentPtr_};
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void WriteVarInt(TSimpleOutputBuffer* writer, T value)
{
    writer->Reserve(MaxVarIntSize<T>);
    auto size = NYT::WriteVarInt(writer->Current(), value);
    writer->Advance(size);
}

void DoPackValuesTyped(TRange<TUnversionedValue> values, TRange<char> discard, EValueType type, TSimpleOutputBuffer* output)
{
    using namespace NYson::NDetail;

    output->PushBack(BeginListSymbol);

    int index = 0;

    auto writeList = [&] (auto onItem) {
        for (const auto& valueArg : values) {
            if (discard[index++]) {
                continue;
            }

            if (valueArg.Type == type) {
                onItem(valueArg);
            } else if (valueArg.Type == EValueType::Null) {
                output->PushBack(EntitySymbol);
            } else {
                THROW_ERROR_EXCEPTION("Unexpected type");
            }
            output->PushBack(ItemSeparatorSymbol);
        }
    };

    switch (type) {
        case EValueType::Int64:
            writeList([&] (const auto& valueArg) {
                output->PushBack(Int64Marker);
                WriteVarInt(output, valueArg.Data.Int64);
            });
            break;
        case EValueType::Uint64:
            writeList([&] (const auto& valueArg) {
                output->PushBack(Uint64Marker);
                WriteVarInt(output, valueArg.Data.Uint64);
            });
            break;
        case EValueType::Double:
            writeList([&] (const auto& valueArg) {
                output->PushBack(DoubleMarker);
                output->Write(&valueArg.Data.Double, sizeof(double));
            });
            break;
        case EValueType::Boolean:
            writeList([&] (const auto& valueArg) {
                output->PushBack(valueArg.Data.Boolean ? TrueMarker : FalseMarker);
            });
            break;
        case EValueType::String:
            writeList([&] (const auto& valueArg) {
                auto stringData = valueArg.AsStringBuf();
                output->PushBack(StringMarker);
                WriteVarInt(output, static_cast<i32>(stringData.length()));
                output->Write(stringData.begin(), stringData.length());
            });
            break;
        case EValueType::Any:
        case EValueType::Composite:
            writeList([&] (const auto& valueArg) {
                auto stringData = valueArg.AsStringBuf();
                output->Write(stringData.begin(), stringData.length());
            });
            break;
        default:
            THROW_ERROR_EXCEPTION("Unexpected type %Qlv",
                type);
    }

    output->PushBack(EndListSymbol);
}

////////////////////////////////////////////////////////////////////////////////

// Build YSON list from values.
TUnversionedValue TNestedTableMerger::PackValuesFast(
    TRange<TUnversionedValue> values,
    TRange<char> discard,
    EValueType type,
    TChunkedMemoryPool* memoryPool)
{
    PackBuffer_.Clear();
    DoPackValuesTyped(values, discard, type, &PackBuffer_);
    auto result = MakeUnversionedCompositeValue(PackBuffer_.GetBuffer());

    // Do not use TRowBuffer because of extra memory tracking.
    char* dst = memoryPool->AllocateUnaligned(result.Length);
    memcpy(dst, result.Data.String, result.Length);
    result.Data.String = dst;

    return result;
}

TUnversionedValue PackValues(TRange<TUnversionedValue> values, TRange<char> discard, EValueType /*type*/, TChunkedMemoryPool* memoryPool)
{
    TString resultYson;
    TStringOutput output(resultYson);
    NYson::TYsonWriter writer(&output);

    YT_ASSERT(values.size() == discard.size());

    writer.OnBeginList();
    for (int index = 0; index < std::ssize(values); ++index) {
        const auto& valueArg = values[index];

        if (discard[index]) {
            continue;
        }

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

    auto result = MakeUnversionedCompositeValue(resultYson);

    // Do not use TRowBuffer because of extra memory tracking.
    char* dst = memoryPool->AllocateUnaligned(result.Length);
    memcpy(dst, result.Data.String, result.Length);
    result.Data.String = dst;

    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TNestedRowDiscardPolicy::Register(TRegistrar registrar)
{
    registrar.Parameter("discard_rows_with_zero_values", &TThis::DiscardRowsWithZeroValues)
        .Default(false);
    registrar.Parameter("floating_point_tolerance", &TThis::FloatingPointTolerance)
        .GreaterThanOrEqual(0.0)
        .Default(0.0);
}

////////////////////////////////////////////////////////////////////////////////

TNestedTableMerger::TNestedTableMerger(bool orderNestedRows, bool useFastYsonRoutines)
    : OrderNestedRows_(orderNestedRows)
    , UseFastYsonRoutines_(useFastYsonRoutines)
{ }

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
            (UseFastYsonRoutines_ ? UnpackNestedValuesListFast : UnpackNestedValuesList)(
                &unpackedColumn,
                keyColumn[index].AsStringBuf(),
                keyColumnSchema.Type);
        }

        if (keyColumnId == 0) {
            YT_ASSERT(Timestamps_.empty() || timestamp >= Timestamps_.back());
            Timestamps_.push_back(timestamp);
            EndOffsets_.push_back(unpackedColumn.size());
        } else {
            if (Timestamps_[index] != timestamp) {
                THROW_ERROR_EXCEPTION("Timestamp mismatch in nested key columns")
                    << TErrorAttribute("column_id", keyColumnId)
                    << TErrorAttribute("expected", Timestamps_[index])
                    << TErrorAttribute("actual", timestamp);
            }

            if (EndOffsets_[index] != std::ssize(unpackedColumn)) {
                THROW_ERROR_EXCEPTION("Mismatch item count in nested key columns")
                    << TErrorAttribute("column_id", keyColumnId)
                    << TErrorAttribute("expected", EndOffsets_[index])
                    << TErrorAttribute("actual", std::ssize(unpackedColumn));
            }
        }
    }
}

void TNestedTableMerger::Reset(int keyWidth, int mergeStreamCount)
{
    Timestamps_.clear();
    EndOffsets_.clear();
    ResultKeys_.clear();
    ResultValues_.clear();
    Discarded_.clear();

    Timestamps_.reserve(mergeStreamCount);
    UnpackedKeys_.resize(keyWidth);
    for (auto& column : UnpackedKeys_) {
        column.clear();
    }
}

void TNestedTableMerger::UnpackKeyColumns(
    TRange<TRange<TVersionedValue>> keyColumns,
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

    BuildMergeScript();

    ApplyMergeScriptToKeys(keyWidth);
}

void TNestedTableMerger::UnpackKeyColumns(
    TRange<std::vector<TVersionedValue>> keyColumns,
    TRange<TNestedKeyColumn> keyColumnsSchema)
{
    YT_VERIFY(OrderNestedRows_);

    if (keyColumns.Empty()) {
        return;
    }

    int keyWidth = std::ssize(keyColumns);
    auto mergeStreamCount = std::ssize(keyColumns.Front());

    Reset(keyWidth, mergeStreamCount);

    for (ui16 keyColumnId = 0; keyColumnId < keyWidth; ++keyColumnId) {
        UnpackKeyColumn(keyColumnId, mergeStreamCount, keyColumns[keyColumnId], keyColumnsSchema[keyColumnId]);
    }

    // Split each group of values into groups with one value.
    {
        Timestamps_.clear();
        int startOffset = 0;
        for (int i = 0; i < std::ssize(EndOffsets_); ++i) {
            if (startOffset == EndOffsets_[i]) {
                Timestamps_.push_back(i);
            } else {
                while (startOffset < EndOffsets_[i]) {
                    Timestamps_.push_back(i);
                    ++startOffset;
                }
            }
        }
    }

#ifndef NDEBUG
    std::vector<int> newOffsets;
    {
        int startOffset = 0;
        for (int i = 0; i < std::ssize(EndOffsets_); ++i) {
            if (startOffset == EndOffsets_[i]) {
                newOffsets.push_back(startOffset);
            } else {
                while (startOffset < EndOffsets_[i]) {
                    newOffsets.push_back(++startOffset);
                }
            }
        }
    }
#endif

    // Split each group of values into groups with one value. Inplace for EndOffsets_.
    {
        auto originalOffsetCount = std::ssize(EndOffsets_);
        YT_VERIFY(originalOffsetCount <= std::ssize(Timestamps_));
        EndOffsets_.resize(Timestamps_.size());
        std::rotate(EndOffsets_.begin(), EndOffsets_.begin() + originalOffsetCount, EndOffsets_.end());

        auto originalOffsets = TRange(EndOffsets_.data() + EndOffsets_.size() - originalOffsetCount, originalOffsetCount);

        auto resultOffsets = EndOffsets_.begin();

        int startOffset = 0;
        for (int i = 0; i < std::ssize(originalOffsets); ++i) {
            if (startOffset == originalOffsets[i]) {
                *resultOffsets++ = startOffset;
            } else {
                // Last originalOffsets[i] can be modified but to the same value.
                int endOffset = originalOffsets[i];
                while (startOffset < endOffset) {
                    *resultOffsets++ = ++startOffset;
                }
            }
        }

// Extra ifdef for ASAN build.
#ifndef NDEBUG
        YT_ASSERT(EndOffsets_ == newOffsets);
#endif
    }

    YT_VERIFY(EndOffsets_.size() == Timestamps_.size());

    BuildMergeScript();

    ApplyMergeScriptToKeys(keyWidth);
}

void TNestedTableMerger::UnpackValueColumn(
    TRange<TVersionedValue> values,
    EValueType elementType,
    TAggregateFunction* aggregateFunction)
{
    // Unpack values.
    auto& unpackedValues = ValueBuffer_;
    unpackedValues.clear();
    CurrentOffsets_.assign(Timestamps_.size(), -1);

    YT_VERIFY(values.Size() <= Timestamps_.size());

    for (const auto& value : values) {
        auto timestampIt = LowerBound(Timestamps_.begin(), Timestamps_.end(), value.Timestamp);

        if (timestampIt == Timestamps_.end() && *timestampIt != value.Timestamp) {
            THROW_ERROR_EXCEPTION("Cannot find matching timestamp for value column")
                << TErrorAttribute("timestamp", value.Timestamp);
        }

        if (value.Type == EValueType::Null) {
            continue;
        }

        int id = timestampIt - Timestamps_.begin();

        auto savedSize = unpackedValues.size();
        // TODO(lukyan): Set aggregate flag from initial versioned value.
        (UseFastYsonRoutines_ ? UnpackNestedValuesListFast : UnpackNestedValuesList)(&unpackedValues, value.AsStringBuf(), elementType);

        CurrentOffsets_[id++] = savedSize++;

        if (OrderNestedRows_) {
            while (savedSize < unpackedValues.size()) {
                CurrentOffsets_[id++] = savedSize++;
            }
        }
    }

    const auto* rowIdPtr = RowIds_.cbegin();

    ResultValues_.push_back({});
    auto& resultValues = ResultValues_.back();

    for (auto count : NestedRowCounts_) {
        // Ids and timestamps in increasing order.

        auto rowId = *rowIdPtr++;

        TUnversionedValue mergeState = MakeUnversionedNullValue();

        // Do not merge if CurrentOffsets_[id] is -1 (npos).
        if (CurrentOffsets_[rowId] != -1) {
            mergeState = unpackedValues[CurrentOffsets_[rowId]++];
        }

        for (int index = 1; index < count; ++index) {
            rowId = *rowIdPtr++;
            auto value = MakeUnversionedNullValue();
            // Do not merge if CurrentOffsets_[id] is -1 (npos).
            if (CurrentOffsets_[rowId] != -1) {
                value = unpackedValues[CurrentOffsets_[rowId]++];
            }
            (*aggregateFunction)(&mergeState, value);
        }

        resultValues.push_back(mergeState);
    }
}

void TNestedTableMerger::DiscardZeroes(const TNestedRowDiscardPolicyPtr& nestedRowDiscardPolicy)
{
    if (!nestedRowDiscardPolicy || !nestedRowDiscardPolicy->DiscardRowsWithZeroValues || ResultValues_.empty()) {
        Discarded_.resize(NestedRowCounts_.size(), false);
        return;
    }

    Discarded_.resize(NestedRowCounts_.size(), true);

    auto shouldDiscard = [&] (TUnversionedValue value) {
        switch (value.Type) {
            case EValueType::Int64:
                return value.Data.Int64 == 0;
                break;

            case EValueType::Uint64:
                return value.Data.Uint64 == 0;
                break;

            case EValueType::Double:
                return std::abs(value.Data.Double) < nestedRowDiscardPolicy->FloatingPointTolerance;
                break;

            default:
                YT_ABORT();
        }
    };

    for (const auto& valueColumn : ResultValues_) {
        YT_ASSERT(valueColumn.size() == NestedRowCounts_.size());

        for (int index = 0; index < std::ssize(NestedRowCounts_); ++index) {
            Discarded_[index] = static_cast<bool>(Discarded_[index]) && shouldDiscard(valueColumn[index]);
        }
    }
}

TVersionedValue TNestedTableMerger::GetPackedKeyColumn(
    int index,
    NTableClient::EValueType type,
    TChunkedMemoryPool* memoryPool)
{
    TVersionedValue value;
    static_cast<TUnversionedValue&>(value) = UseFastYsonRoutines_
        ? PackValuesFast(ResultKeys_[index], Discarded_, type, memoryPool)
        : PackValues(ResultKeys_[index], Discarded_, type, memoryPool);

    value.Timestamp = Timestamps_.back();

    return value;
}

TVersionedValue TNestedTableMerger::GetPackedValueColumn(
    int index,
    NTableClient::EValueType type,
    TChunkedMemoryPool* memoryPool)
{
    TVersionedValue value;
    static_cast<TUnversionedValue&>(value) = UseFastYsonRoutines_
        ? PackValuesFast(ResultValues_[index], Discarded_, type, memoryPool)
        : PackValues(ResultValues_[index], Discarded_, type, memoryPool);

    value.Timestamp = Timestamps_.back();

    return value;
}

void TNestedTableMerger::BuildMergeScript()
{
    RowIds_.clear();
    NestedRowCounts_.clear();

    if (EndOffsets_.empty()) {
        return;
    }

    int keyWidth = std::ssize(UnpackedKeys_);

    // Make heap to merge values.
    {
        CurrentOffsets_.resize(std::ssize(EndOffsets_));
        int startOffset = 0;
        for (int index = 0; index < std::ssize(CurrentOffsets_); ++index) {
            CurrentOffsets_[index] = startOffset;
            startOffset = EndOffsets_[index];
        }
    }

    auto compareKey = [&] (int lhsIndex, int rhsIndex) {
        for (ui16 keyColumnId = 0; keyColumnId < keyWidth; ++keyColumnId) {
            if (UnpackedKeys_[keyColumnId][lhsIndex] != UnpackedKeys_[keyColumnId][rhsIndex]) {
                return UnpackedKeys_[keyColumnId][lhsIndex] < UnpackedKeys_[keyColumnId][rhsIndex] ? -1 : 1;
            }
        }

        return 0;
    };

    // Returns true once stream is depleted.
    auto advanceStream = [&] (int id) {
        auto endOffset = EndOffsets_[id];
        YT_VERIFY(CurrentOffsets_[id] != endOffset);

        return ++CurrentOffsets_[id] == endOffset;
    };

    RowIdHeap_.clear();

    for (int id = 0; id < std::ssize(CurrentOffsets_); ++id) {
        if (CurrentOffsets_[id] != EndOffsets_[id]) {
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

void TNestedTableMerger::ApplyMergeScriptToKeys(int keyWidth)
{
    ResultKeys_.resize(keyWidth);
    for (int index = 0; index < keyWidth; ++index) {
        {
            CurrentOffsets_.resize(std::ssize(EndOffsets_));
            int startOffset = 0;
            for (int index = 0; index < std::ssize(CurrentOffsets_); ++index) {
                CurrentOffsets_[index] = startOffset;
                startOffset = EndOffsets_[index];
            }
        }

        const auto* idPtr = RowIds_.begin();
        for (auto count : NestedRowCounts_) {
            // Ids and timestamps are in increasing order.

            auto id = *idPtr++;
            ResultKeys_[index].push_back(UnpackedKeys_[index][CurrentOffsets_[id]++]);

            for (int i = 1; i < count; ++i) {
                ++CurrentOffsets_[*idPtr++];
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

std::vector<int> GetMissingNestedKeyColumnsIfNeeded(
    const TColumnFilter& columnFilter,
    const TNestedColumnsSchema& nestedSchema)
{
    if (columnFilter.IsUniversal()) {
        return {};
    }

    THashSet<int> filtered(columnFilter.GetIndexes().begin(), columnFilter.GetIndexes().end());
    std::vector<int> missingNestedKeyColumns;

    for (const auto& column : nestedSchema.KeyColumns) {
        if (!filtered.contains(column.Id)) {
            missingNestedKeyColumns.push_back(column.Id);
        }
    }

    if (missingNestedKeyColumns.size() != nestedSchema.KeyColumns.size()) {
        return missingNestedKeyColumns;
    }

    for (const auto& column : nestedSchema.ValueColumns) {
        if (filtered.contains(column.Id)) {
            return missingNestedKeyColumns;
        }
    }

    return {};
}

TColumnFilter EnrichColumnFilter(
    const TColumnFilter& columnFilter,
    const TNestedColumnsSchema& nestedSchema,
    int requiredKeyColumnCount)
{
    if (columnFilter.IsUniversal()) {
        return {};
    }

    auto indexes = columnFilter.GetIndexes();

    auto insertedNestedKeyColumns = GetMissingNestedKeyColumnsIfNeeded(columnFilter, nestedSchema);

    for (int index = 0; index < requiredKeyColumnCount; ++index) {
        indexes.push_back(index);
    }

    std::move(insertedNestedKeyColumns.begin(), insertedNestedKeyColumns.end(), std::back_inserter(indexes));

    std::sort(indexes.begin(), indexes.end());
    indexes.erase(std::unique(indexes.begin(), indexes.end()), indexes.end());

    return TColumnFilter({std::move(indexes)});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRowMerger
