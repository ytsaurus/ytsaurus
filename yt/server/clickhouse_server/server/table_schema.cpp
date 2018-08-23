#include "table_schema.h"

#include "types_translation.h"

#include <yt/server/clickhouse_server/yql_helpers/yql_row_spec.h>

#include <yt/client/table_client/schema.h>

#include <yt/core/yson/string.h>

namespace NYT {
namespace NClickHouse {

using namespace NYT::NTableClient;
using namespace NYT::NYson;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTableSchemaBuilder
{
private:
    NInterop::TColumnList Columns;

public:
    TTableSchemaBuilder()
    {}

    // return false if column skipped
    bool AddColumn(const TColumnSchema& columnSchema);
    bool AddColumn(const TColumnSchema& schema, TStringBuf yqlTypeName);

    NInterop::TColumnList GetColumns()
    {
        return std::move(Columns);
    }

private:
    static bool AreTypesConsistent(EValueType ytValueType, TStringBuf yqlTypeName);
};

////////////////////////////////////////////////////////////////////////////////

bool TTableSchemaBuilder::AddColumn(const TColumnSchema& ytColumn)
{
    auto ytPhysicalType = ytColumn.GetPhysicalType();

    if (!IsYtTypeSupported(ytPhysicalType)) {
        // skip unsupported type
        return false;
    }

    NInterop::TColumn column;
    column.Name = ytColumn.Name();
    column.Type = RepresentYtType(ytPhysicalType);
    if (ytColumn.SortOrder().HasValue()) {
        column.SetSorted();
    }
    Columns.push_back(std::move(column));
    return true;
}

bool TTableSchemaBuilder::AddColumn(const TColumnSchema& ytColumn, const TStringBuf yqlTypeName)
{
    if (!IsYqlTypeSupported(yqlTypeName)) {
        // ignore YQL type spec
        return AddColumn(ytColumn);
    }

    auto ytPhysicalType = ytColumn.GetPhysicalType();

    if (!AreTypesConsistent(ytPhysicalType, yqlTypeName)) {
        THROW_ERROR_EXCEPTION(
            "Inconsistent %Qlv column types: YT - %Qlv, YQL - %Qlv",
            ytColumn.Name(), ytPhysicalType, yqlTypeName);
    }

    NInterop::TColumn column;
    column.Name = ytColumn.Name();
    column.Type = RepresentYqlType(yqlTypeName);
    if (ytColumn.SortOrder().HasValue()) {
        column.SetSorted();
    }
    Columns.push_back(std::move(column));
    return true;
}

bool TTableSchemaBuilder::AreTypesConsistent(EValueType ytValueType, TStringBuf yqlTypeName)
{
    return GetYqlUnderlyingYtType(yqlTypeName) == ytValueType;
}

////////////////////////////////////////////////////////////////////////////////

TMaybe<TStringBuf> GetColumnYqlType(const NYql::TYqlRowSpecInfo* yqlRowSpec, const TString& columnName)
{
    if (!yqlRowSpec) {
        return Nothing();
    }

    auto index = yqlRowSpec->Type->FindItem(columnName);
    if (!index) {
        return Nothing();
    }

    const auto* typeNode = yqlRowSpec->Type->GetItems()[*index]->GetItemType();
    if (static_cast<int>(typeNode->GetKind()) != static_cast<int>(NYql::ETypeAnnotationKind::Data)) {
         THROW_ERROR_EXCEPTION("Invalid YQL schema: invalid column")
             << TErrorAttribute("column", columnName);
    }

    const auto* dataType = typeNode->Cast<NYql::TDataExprType>();
    return dataType->GetDataType();
}

NInterop::TTablePtr CreateTableSchema(
    const TString& name,
    const TTableSchema& schema,
    const NYql::TYqlRowSpecInfo* yqlRowSpec)
{
    TTableSchemaBuilder schemaBuilder;

    for (const auto& columnSchema: schema.Columns()) {
        auto yqlTypeName = GetColumnYqlType(yqlRowSpec, columnSchema.Name());
        if (yqlTypeName.Defined()) {
            schemaBuilder.AddColumn(columnSchema, *yqlTypeName);
        } else {
            schemaBuilder.AddColumn(columnSchema);
        }
    }

    return std::make_shared<NInterop::TTable>(name, schemaBuilder.GetColumns());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NInterop::TTablePtr CreateTableSchema(
    const TString& name,
    const TTableSchema& schema,
    const TYsonString& yqlSchema)
{
    if (yqlSchema) {
        NYql::TExprContext ctx;
        NYql::TYqlRowSpecInfo rowSpecInfo;
        if (!rowSpecInfo.Parse(yqlSchema.GetData(), ctx)) {
            THROW_ERROR_EXCEPTION("Unable to parse YQL schema")
                << TErrorAttribute("issues", ctx.IssueManager.GetIssues().ToString());
        }
        return CreateTableSchema(name, schema, &rowSpecInfo);
    }

    return CreateTableSchema(name, schema, nullptr);
}

}   // namespace NClickHouse
}   // namespace NYT
