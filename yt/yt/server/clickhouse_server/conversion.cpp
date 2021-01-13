#include "conversion.h"

#include "yt_ch_converter.h"
#include "config.h"

#include <yt/client/table_client/schema.h>

#include <library/cpp/iterator/functools.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>

#include <Storages/ColumnsDescription.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

DB::DataTypePtr ToDataType(const TComplexTypeFieldDescriptor& descriptor, const TCompositeSettingsPtr& settings)
{
    TYTCHConverter converter(descriptor, settings);
    return converter.GetDataType();
}

DB::DataTypes ToDataTypes(const NTableClient::TTableSchema& schema, const TCompositeSettingsPtr& settings)
{
    DB::DataTypes result;
    result.reserve(schema.GetColumnCount());

    for (const auto& column : schema.Columns()) {
        TComplexTypeFieldDescriptor descriptor(column);
        result.emplace_back(ToDataType(std::move(descriptor), settings));
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

TLogicalTypePtr ToLogicalType(const DB::DataTypePtr& type)
{
    // TODO(max42): rewrite this using CH -> YT converter.
    if (type->isNullable()) {
        return OptionalLogicalType(ToLogicalType(DB::removeNullable(type)));
    }
    switch (type->getTypeId()) {
        case DB::TypeIndex::Int64:
            return SimpleLogicalType(ESimpleLogicalValueType::Int64);
        case DB::TypeIndex::Int32:
            return SimpleLogicalType(ESimpleLogicalValueType::Int32);
        case DB::TypeIndex::Int16:
            return SimpleLogicalType(ESimpleLogicalValueType::Int16);
        case DB::TypeIndex::Int8:
            return SimpleLogicalType(ESimpleLogicalValueType::Int8);
        case DB::TypeIndex::UInt64:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint64);
        case DB::TypeIndex::UInt32:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint32);
        case DB::TypeIndex::UInt16:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint16);
        case DB::TypeIndex::UInt8:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint8);
        case DB::TypeIndex::Float32:
        case DB::TypeIndex::Float64:
            return SimpleLogicalType(ESimpleLogicalValueType::Double);
        case DB::TypeIndex::String:
        case DB::TypeIndex::FixedString:
            return SimpleLogicalType(ESimpleLogicalValueType::String);
        case DB::TypeIndex::Date:
            return SimpleLogicalType(ESimpleLogicalValueType::Date);
        case DB::TypeIndex::DateTime:
            return SimpleLogicalType(ESimpleLogicalValueType::Datetime);
        // TODO(dakovalkov): https://github.com/yandex/ClickHouse/pull/7170.
        // case DB::TypeIndex::DateTime64:
        //     return SimpleLogicalType(ESimpleLogicalValueType::Timestamp);
        default:
            THROW_ERROR_EXCEPTION("ClickHouse type %Qv is not supported", type->getFamilyName());
    }
}

TTableSchema ToTableSchema(const DB::ColumnsDescription& columns, const TKeyColumns& keyColumns)
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
        const auto& type = ToLogicalType(column.type);
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
