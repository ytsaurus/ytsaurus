
#include <library/cpp/yt/yson/consumer.h>

#include <yt/systest/table.h>
#include <yt/yt/client/api/public.h>
#include <yt/yt/client/api/table_reader.h>

namespace NYT::NTest {

static NTableClient::TTableSchema ToSchema(const TTable& table)
{
    std::vector<NTableClient::TColumnSchema> columns;
    std::vector<NTableClient::TDeletedColumn> deletedColumns;

    for (int i = 0; i < std::ssize(table.DataColumns); ++i) {
        const auto& column = table.DataColumns[i];
        std::optional<NTableClient::ESortOrder> sortOrder;
        if (i < table.SortColumns) {
            sortOrder = NTableClient::ESortOrder::Ascending;
        }
        NTableClient::TColumnSchema columnSchema(column.Name, GetType(column.Type), sortOrder);
        if (column.StableName) {
            columnSchema.SetStableName(NTableClient::TColumnStableName(*column.StableName));
        }
        columns.push_back(columnSchema);
    }

    for (const auto& deletedColumnName : table.DeletedColumnNames) {
        deletedColumns.push_back(NTableClient::TDeletedColumn(NTableClient::TColumnStableName(deletedColumnName)));
    }

    return NTableClient::TTableSchema(columns, true, false, NTableClient::ETableSchemaModification::None, deletedColumns);
}

TString SchemaTypeName(NProto::EColumnType type)
{
    switch (type) {
        case NProto::EColumnType::ENone:
            return "(none)";
        case NProto::EColumnType::EInt8:
            return "int8";
        case NProto::EColumnType::EInt16:
            return "int16";
        case NProto::EColumnType::EInt64:
            return "int64";
        case NProto::EColumnType::ELatinString100:
        case NProto::EColumnType::EBytes64K:
            return "string";
        case NProto::EColumnType::EDouble:
            return "double";
    }
}

NTableClient::ESimpleLogicalValueType GetType(NProto::EColumnType type)
{
    switch (type) {
        case NProto::EColumnType::ENone:
            return NTableClient::ESimpleLogicalValueType::Null;
        case NProto::EColumnType::EInt8:
            return NTableClient::ESimpleLogicalValueType::Int8;
        case NProto::EColumnType::EInt16:
            return NTableClient::ESimpleLogicalValueType::Int16;
        case NProto::EColumnType::EInt64:
            return NTableClient::ESimpleLogicalValueType::Int64;
        case NProto::EColumnType::ELatinString100:
            return NTableClient::ESimpleLogicalValueType::String;  // utf-8
        case NProto::EColumnType::EBytes64K:
            return NTableClient::ESimpleLogicalValueType::String;
        case NProto::EColumnType::EDouble:
            return NTableClient::ESimpleLogicalValueType::Double;
    }
}

////////////////////////////////////////////////////////////////////////////////

TTable DropStableNames(const TTable& table)
{
    TTable result;
    for (const auto& column : table.DataColumns) {
        result.DataColumns.push_back({
            column.Name,
            column.Type,
            std::nullopt
        });
    }
    result.SortColumns = table.SortColumns;
    return result;
}

TString BuildAttributes(const TTable& table)
{
    TString attrs("<schema=[");
    int i = 0;
    for (const auto& column : table.DataColumns) {
        if (i > 0) {
            attrs += ";";
        }
        attrs += "{name=";
        attrs += column.Name;
        attrs += ";type=";
        attrs += SchemaTypeName(column.Type);
        if (i < table.SortColumns) {
            attrs += ";sort_order=ascending";
        }
        attrs += "}";
        ++i;
    }
    for (const auto& column : table.DeletedColumnNames) {
        if (i > 0) {
            attrs += ";";
        }
        attrs += "{stable_name=";
        attrs += column;
        attrs += ";deleted=%true}";
    }
    attrs += "]>";
    return attrs;
}

void ToProto(NProto::TDataColumn* proto, const TDataColumn& column)
{
    proto->set_name(column.Name);
    proto->set_type(column.Type);
    if (column.StableName) {
        proto->set_stable_name(*column.StableName);
    }
}

void FromProto(TDataColumn* column, const NProto::TDataColumn& proto)
{
    column->Name = proto.name();
    column->Type = proto.type();
    if (proto.has_stable_name()) {
        column->StableName = proto.stable_name();
    }
}

void FromProto(TTable* table, const NProto::TTable& proto)
{
    table->DataColumns.clear();
    for (const auto& protoColumn : proto.columns()) {
        TDataColumn column;
        FromProto(&column, protoColumn);
        table->DataColumns.push_back(std::move(column));
    }
    table->DeletedColumnNames.clear();
    for (const auto& name : proto.deleted_column_name()) {
        table->DeletedColumnNames.push_back(name);
    }
    table->SortColumns = proto.sort_columns();
}

void ToProto(NProto::TTable* proto, const TTable &table)
{
    for (const auto& column : table.DataColumns) {
        ToProto(proto->add_columns(), column);
    }
    for (const auto& name : table.DeletedColumnNames) {
        proto->add_deleted_column_name(name);
    }
    proto->set_sort_columns(table.SortColumns);
}

void AlterTable(NApi::IClientPtr client, const TString& path, const TTable& table)
{
    NApi::TAlterTableOptions options;
    options.Schema = ToSchema(table);
    NYT::NLogging::TLogger Logger("test");

    YT_LOG_INFO("Alter table (Path: %v, Schema: %v", path, options.Schema);
    NConcurrency::WaitFor(client->AlterTable(path, options)).ThrowOnError();
}

}  // namespace NYT::NTest
