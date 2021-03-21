#include "conversion.h"

#include "yt_ch_converter.h"
#include "ch_yt_converter.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/row_buffer.h>

#include <library/cpp/iterator/functools.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>

#include <Storages/ColumnsDescription.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;
using namespace NLogging;

static const TLogger Logger("Conversion");

////////////////////////////////////////////////////////////////////////////////

DB::DataTypePtr ToDataType(const TComplexTypeFieldDescriptor& descriptor, const TCompositeSettingsPtr& settings, bool enableReadOnlyConversions)
{
    TYTCHConverter converter(descriptor, settings, enableReadOnlyConversions);
    return converter.GetDataType();
}

DB::DataTypes ToDataTypes(const NTableClient::TTableSchema& schema, const TCompositeSettingsPtr& settings, bool enableReadOnlyConversions)
{
    DB::DataTypes result;
    result.reserve(schema.GetColumnCount());

    for (const auto& column : schema.Columns()) {
        TComplexTypeFieldDescriptor descriptor(column);
        result.emplace_back(ToDataType(std::move(descriptor), settings, enableReadOnlyConversions));
    }

    return result;
}

DB::NamesAndTypesList ToNamesAndTypesList(const NTableClient::TTableSchema& schema, const TCompositeSettingsPtr& settings)
{
    const auto& dataTypes = ToDataTypes(schema, settings);

    DB::NamesAndTypesList result;

    for (int index = 0; index < schema.GetColumnCount(); ++index) {
        result.emplace_back(schema.Columns()[index].Name(), dataTypes[index]);
    }

    return result;
}

DB::Block ToHeaderBlock(const TTableSchema& schema, const TCompositeSettingsPtr& settings)
{
    DB::Block headerBlock;

    auto namesAndTypesList = ToNamesAndTypesList(schema, settings);

    for (const auto& nameAndTypePair : namesAndTypesList) {
        auto column = nameAndTypePair.type->createColumn();
        headerBlock.insert({ std::move(column), nameAndTypePair.type, nameAndTypePair.name });
    }

    return headerBlock;
}

////////////////////////////////////////////////////////////////////////////////

EValueType ToValueType(DB::Field::Types::Which which)
{
    switch (which) {
        case DB::Field::Types::Which::Null:
            return EValueType::Null;
        case DB::Field::Types::Which::Int64:
            return EValueType::Int64;
        case DB::Field::Types::Which::UInt64:
            return EValueType::Uint64;
        case DB::Field::Types::Which::Float64:
            return EValueType::Double;
        case DB::Field::Types::Which::String:
            return EValueType::String;
        default:
            THROW_ERROR_EXCEPTION(
                "ClickHouse physical type %Qv is not supported",
                DB::Field::Types::toString(which));
    }
}

////////////////////////////////////////////////////////////////////////////////

TLogicalTypePtr ToLogicalType(const DB::DataTypePtr& type, const TCompositeSettingsPtr& settings)
{
    TCHYTConverter converter(type, settings);
    return converter.GetLogicalType();
}

TTableSchema ToTableSchema(const DB::ColumnsDescription& columns, const TKeyColumns& keyColumns, const TCompositeSettingsPtr& settings)
{
    std::vector<TString> columnOrder;
    THashSet<TString> usedColumns;

    for (const auto& keyColumnName : keyColumns) {
        if (!columns.has(keyColumnName)) {
            THROW_ERROR_EXCEPTION("Column %Qv is specified as key column but is missing",
                keyColumnName);
        }
        columnOrder.emplace_back(keyColumnName);
        usedColumns.emplace(keyColumnName);
    }

    for (const auto& column : columns) {
        if (usedColumns.emplace(column.name).second) {
            columnOrder.emplace_back(column.name);
        }
    }

    std::vector<TColumnSchema> columnSchemas;
    columnSchemas.reserve(columnOrder.size());
    for (int index = 0; index < static_cast<int>(columnOrder.size()); ++index) {
        const auto& name = columnOrder[index];
        const auto& column = columns.get(name);
        const auto& type = ToLogicalType(column.type, settings);
        std::optional<ESortOrder> sortOrder;
        if (index < static_cast<int>(keyColumns.size())) {
            sortOrder = ESortOrder::Ascending;
        }
        columnSchemas.emplace_back(name, type, sortOrder);
    }

    return TTableSchema(columnSchemas);
}

////////////////////////////////////////////////////////////////////////////////

// TODO(dakovalkov): value.Type is a physical type, it's better to pass the desired CH-type.
DB::Field ToField(const NTableClient::TUnversionedValue& value)
{
    switch (value.Type) {
        case EValueType::Null:
            return DB::Field();
        case EValueType::Int64:
            return DB::Field(static_cast<DB::Int64>(value.Data.Int64));
        case EValueType::Uint64:
            return DB::Field(static_cast<DB::UInt64>(value.Data.Uint64));
        case EValueType::Double:
            return DB::Field(static_cast<DB::Float64>(value.Data.Double));
        case EValueType::Boolean:
            return DB::Field(static_cast<DB::UInt64>(value.Data.Boolean ? 1 : 0));
        case EValueType::String:
        case EValueType::Any:
        case EValueType::Composite:
            return DB::Field(value.Data.String, value.Length);
        default:
            THROW_ERROR_EXCEPTION("Unexpected data type %Qlv", value.Type);
    }
}

void ToUnversionedValue(const DB::Field& field, TUnversionedValue* value)
{
    value->Type = ToValueType(field.getType());
    switch (value->Type) {
        case EValueType::Int64:
        case EValueType::Uint64:
        case EValueType::Double: {
            memcpy(&value->Data, &field.reinterpret<ui64>(), sizeof(value->Data));
            break;
        }
        case EValueType::Boolean: {
            if (field.get<ui64>() > 1) {
                THROW_ERROR_EXCEPTION("Cannot convert value %v to boolean", field.get<ui64>());
            }
            memcpy(&value->Data, &field.get<ui64>(), sizeof(value->Data));
            break;
        }
        case EValueType::String: {
            const auto& str = field.get<std::string>();
            value->Data.String = str.data();
            value->Length = str.size();
            break;
        }
        default: {
            THROW_ERROR_EXCEPTION("Unexpected data type %Qlv", value->Type);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

DB::Block ToBlock(
    const IUnversionedRowBatchPtr& batch,
    const TTableSchema& readSchema,
    const std::vector<int>& idToColumnIndex,
    const TRowBufferPtr& rowBuffer,
    const DB::Block& headerBlock,
    const TCompositeSettingsPtr& compositeSettings)
{
    // NB(max42): CHYT-256.
    // If chunk schema contains not all of the requested columns (which may happen
    // when a non-required column was introduced after chunk creation), we are not
    // going to receive some of the unversioned values with nulls. We still need
    // to provide them to CH, though, so we keep track of present columns for each
    // row we get and add nulls for all unpresent columns.
    std::vector<bool> presentColumnMask(readSchema.GetColumnCount());

    auto block = headerBlock.cloneEmpty();

    // Indexed by column indices.
    std::vector<TYTCHConverter> converters;
    converters.reserve(readSchema.GetColumnCount());

    for (int columnIndex = 0; columnIndex < readSchema.GetColumnCount(); ++columnIndex) {
        const auto& columnSchema = readSchema.Columns()[columnIndex];
        TComplexTypeFieldDescriptor descriptor(columnSchema);
        converters.emplace_back(descriptor, compositeSettings);
    }

    if (auto columnarBatch = batch->TryAsColumnar()) {
        auto batchColumns = columnarBatch->MaterializeColumns();
        for (const auto* ytColumn : batchColumns) {
            auto columnIndex = idToColumnIndex[ytColumn->Id];
            YT_VERIFY(columnIndex != -1);
            converters[columnIndex].ConsumeYtColumn(*ytColumn);
            presentColumnMask[columnIndex] = true;
        }
        for (int columnIndex = 0; columnIndex < static_cast<int>(readSchema.Columns().size()); ++columnIndex) {
            if (!presentColumnMask[columnIndex]) {
                YT_VERIFY(!readSchema.Columns()[columnIndex].Required());
                converters[columnIndex].ConsumeNulls(batch->GetRowCount());
            }
        }
    } else {
        auto rowBatch = batch->MaterializeRows();
        // We transpose rows by writing down contiguous range of values for each column.
        // This is done to reduce the number of converter virtual calls.
        std::vector<std::vector<TUnversionedValue>> columnIndexToUnversionedValues(readSchema.GetColumnCount());
        for (auto& unversionedValues : columnIndexToUnversionedValues) {
            unversionedValues.reserve(rowBatch.size());
        }

        auto nullValue = MakeUnversionedNullValue();

        for (auto row : rowBatch) {
            presentColumnMask.assign(readSchema.GetColumnCount(), false);
            for (int index = 0; index < static_cast<int>(row.GetCount()); ++index) {
                auto value = row[index];
                auto id = value.Id;
                int columnIndex = (id < idToColumnIndex.size()) ? idToColumnIndex[id] : -1;
                YT_VERIFY(columnIndex != -1);
                YT_VERIFY(!presentColumnMask[columnIndex]);
                presentColumnMask[columnIndex] = true;
                columnIndexToUnversionedValues[columnIndex].emplace_back(value);
            }
            for (int columnIndex = 0; columnIndex < readSchema.GetColumnCount(); ++columnIndex) {
                if (!presentColumnMask[columnIndex]) {
                    YT_VERIFY(readSchema.Columns()[columnIndex].LogicalType()->IsNullable());
                    // NB: converter does not care about value ids.
                    columnIndexToUnversionedValues[columnIndex].emplace_back(nullValue);
                }
            }
        }

        for (int columnIndex = 0; columnIndex < readSchema.GetColumnCount(); ++columnIndex) {
            const auto& unversionedValues = columnIndexToUnversionedValues[columnIndex];
            YT_VERIFY(unversionedValues.size() == rowBatch.size());
            auto& converter = converters[columnIndex];
            converter.ConsumeUnversionedValues(unversionedValues);
        }
    }

    for (const auto& [columnIndex, converter] : Enumerate(converters)) {
        auto column = converter.FlushColumn();
        YT_VERIFY(column->size() == batch->GetRowCount());
        block.getByPosition(columnIndex).column = std::move(column);
    }

    return block;
}

TSharedRange<TUnversionedRow> ToRowRange(
    const DB::Block& block,
    const std::vector<DB::DataTypePtr>& dataTypes,
    const std::vector<int>& columnIndexToId,
    const TCompositeSettingsPtr& settings)
{
    int columnCount = columnIndexToId.size();
    i64 rowCount = block.rows();
    const auto& columns = block.getColumns();
    YT_VERIFY(columns.size() == columnCount);

    std::vector<TCHYTConverter> converters;
    converters.reserve(columnCount);
    for (int columnIndex = 0; columnIndex < columnCount; ++columnIndex) {
        converters.emplace_back(dataTypes[columnIndex], settings);
    }

    TRowBufferPtr rowBuffer = New<TRowBuffer>();
    std::vector<TMutableUnversionedRow> mutableRows(rowCount);

    for (i64 rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        mutableRows[rowIndex] = rowBuffer->AllocateUnversioned(columnCount);
    }

    for (int columnIndex = 0; columnIndex < columnCount; ++columnIndex) {
        auto& converter = converters[columnIndex];
        auto valueRange = converter.ConvertColumnToUnversionedValues(columns[columnIndex]);
        int id = columnIndexToId[columnIndex];
        YT_VERIFY(valueRange.size() == rowCount);
        for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
            auto& value = mutableRows[rowIndex][columnIndex];
            value = valueRange[rowIndex];
            value.Id = id;
        }
    }

    std::vector<TUnversionedRow> rows(rowCount);
    for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex) {
        rows[rowIndex] = mutableRows[rowIndex];
    }

    // Rows are backed up by row buffer, string data is backed up converters (which
    // hold original columns if necessary).
    return MakeSharedRange(rows, std::move(converters), std::move(rowBuffer));
}

////////////////////////////////////////////////////////////////////////////////

TClickHouseKeys ToClickHouseKeys(
    const TLegacyKey& lowerKey,
    const TLegacyKey& upperKey,
    // How many columns are used in key condition.
    int usedKeyColumnCount,
    const DB::DataTypes& dataTypes,
    bool makeUpperBoundInclusive)
{
    YT_VERIFY(usedKeyColumnCount <= dataTypes.size());

    // XXX(dakovalkov): CH does not support nullable key columns, so we use several dirty tricks to represent them as not nullable.
    DB::DataTypes notNullableDataTypes(usedKeyColumnCount);
    for (int index = 0; index < usedKeyColumnCount; ++index) {
        notNullableDataTypes[index] = DB::removeNullable(dataTypes[index]);
    }

    int commonPrefixSize = 0;
    while (commonPrefixSize < lowerKey.GetCount()
        && commonPrefixSize < upperKey.GetCount()
        && lowerKey[commonPrefixSize] == upperKey[commonPrefixSize])
    {
        ++commonPrefixSize;
    }

    TClickHouseKeys result;
    result.MinKey.resize(usedKeyColumnCount);
    result.MaxKey.resize(usedKeyColumnCount);

    // Convert TLegacyKey to ClickHouse FieldRef[].
    auto convertToClickHouseKey = [&] (DB::FieldRef* chKey, const TLegacyKey& ytKey, bool isUpperKey) {
        std::optional<EValueType> sentinel;
        int lastNonSentinelIndex = -1;

        for (int index = 0; index < usedKeyColumnCount; ++index) {
            if (!sentinel) {
                if (index >= ytKey.GetCount() || ytKey[index].Type == EValueType::Min) {
                    sentinel = EValueType::Min;
                } else if (ytKey[index].Type == EValueType::Max) {
                    sentinel = EValueType::Max;
                } else if (!isUpperKey && index >= commonPrefixSize && ytKey[index].Type == EValueType::Null) {
                    // XXX(dakovalkov): Dirty trick (1) to remove null.
                    // For lower key replacing Null with the minimum value can break our boundaries,
                    // so we need to replace all following values with the minimum as well.
                    // See 'Dirty trick (4)' for more details.
                    sentinel = EValueType::Min;
                }
            }

            if (sentinel) {
                // XXX(dakovalkov): Dirty trick (2) to remove sentinels.
                // CH does not support Min/Max sentinels yet, so replace them with min/max values.
                // If min/max value is not representable, replace it with some big/small value.
                // It will work in most cases.
                if (*sentinel == EValueType::Min) {
                    chKey[index] = TryGetMinimumTypeValue(notNullableDataTypes[index]);
                } else {
                    chKey[index] = TryGetMaximumTypeValue(notNullableDataTypes[index]);
                }
            } else {
                if (ytKey[index].Type == EValueType::Null) {
                    // XXX(dakovalkov): Dirty trick (3) to remove null.
                    // Replacing Null in keys with fixed value helps when a meaningful column follows a null column.
                    // For instance, (lowerKey=[Null, 1], upperKey=[Null, 3]) -> (lowerKey=[0, 1], upperKey=[0, 3]).
                    // 'where' condition on second column will work fine in this case.
                    YT_VERIFY(index < commonPrefixSize || isUpperKey);
                    chKey[index] = TryGetMinimumTypeValue(notNullableDataTypes[index]);
                } else {
                    chKey[index] = ToField(ytKey[index]);
                    lastNonSentinelIndex = index;
                    // XXX(dakovalkov): Dirty trick (4) to remove null.
                    // When we replace null with minimum value, we break sort order a litle bit.
                    // For instance, ([Null, 5], [0, 1]) -> ([0, 5], [0, 1]).
                    // If upper key of the chunk is [0, 2] and the query is 'where second_column = 5',
                    // we can wrongly skip this chunk.
                    // To eliminate this, we need to extend upper key with maximum values.
                    if (isUpperKey && index >= commonPrefixSize && dataTypes[index]->isNullable()) {
                        if (chKey[index] == TryGetMinimumTypeValue(notNullableDataTypes[index])) {
                            sentinel = EValueType::Max;
                        }
                    }
                }
            }
        }

        // Trying to convert the exclusive upper bound to inclusive.
        if (isUpperKey && makeUpperBoundInclusive) {
            bool isExclusive;

            if (sentinel) {
                isExclusive = (*sentinel == EValueType::Min);
            } else {
                isExclusive = (ytKey.GetCount() == usedKeyColumnCount);
            }

            if (isExclusive && lastNonSentinelIndex >= 0) {
                std::vector<std::optional<DB::Field>> inclusiveSuffix;
                inclusiveSuffix.reserve(usedKeyColumnCount);

                // We try to decrement last non-sentinel value and convert all min-values after it to max-values.
                // A, B, X, MIN, MIN -> A, B, (X - 1), MAX, MAX

                bool failed = false;
                failed |= !inclusiveSuffix.emplace_back(
                    TryDecrementFieldValue(
                        chKey[lastNonSentinelIndex],
                        notNullableDataTypes[lastNonSentinelIndex]));

                for (int index = lastNonSentinelIndex + 1; !failed && index < usedKeyColumnCount; ++index) {
                    failed |= !inclusiveSuffix.emplace_back(TryGetMaximumTypeValue(notNullableDataTypes[index]));
                }

                if (!failed) {
                    for (int index = lastNonSentinelIndex; index < usedKeyColumnCount; ++index) {
                        chKey[index] = *inclusiveSuffix[index - lastNonSentinelIndex];
                    }
                }
            }
        }
    };

    convertToClickHouseKey(result.MinKey.data(), lowerKey, false);
    convertToClickHouseKey(result.MaxKey.data(), upperKey, true);

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
