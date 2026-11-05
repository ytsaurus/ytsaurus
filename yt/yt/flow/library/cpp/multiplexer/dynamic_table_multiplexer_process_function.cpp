#include "dynamic_table_multiplexer_process_function.h"

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>

#include <util/string/join.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TDynamicTableMultiplexerParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("table_path", &TThis::TablePath)
        .Default();

    registrar.Postprocessor([] (TDynamicTableMultiplexerParameters* parameters) {
        if (parameters->TablePath.GetPath().empty()) {
            THROW_ERROR_EXCEPTION("\"table_path\" must be set and have a non-empty path");
        }
        if (!parameters->TablePath.GetCluster()) {
            THROW_ERROR_EXCEPTION("\"table_path\" must specify a cluster");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

namespace NDynamicTableMultiplexer {

////////////////////////////////////////////////////////////////////////////////

std::pair<NTableClient::TTableSchemaPtr, std::vector<std::string>> SplitTableSchemaByGroupBy(
    const NTableClient::TTableSchema& tableSchema,
    const NTableClient::TTableSchema& groupBySchema)
{
    if (std::ssize(tableSchema.Columns()) <= std::ssize(groupBySchema.Columns())) {
        THROW_ERROR_EXCEPTION("Table schema has fewer columns than group_by_schema")
            .With("table_columns", tableSchema.GetColumnCount())
            .With("group_by_columns", groupBySchema.GetColumnCount());
    }

    for (int i = 0; i < std::ssize(groupBySchema.Columns()); ++i) {
        const auto& tableColumn = tableSchema.Columns()[i];
        const auto& groupByColumn = groupBySchema.Columns()[i];
        if (tableColumn.Name() != groupByColumn.Name()) {
            THROW_ERROR_EXCEPTION(
                "Table schema does not match group_by_schema at position %v: "
                "expected column %Qv, got %Qv",
                i,
                groupByColumn.Name(),
                tableColumn.Name());
        }
        // Compare types ignoring required/optional wrapping: computed columns
        // cannot be marked required, but hold the same underlying type.
        const auto& tableType = *NTableClient::MakeOptionalIfNot(tableColumn.LogicalType());
        const auto& groupByType = *NTableClient::MakeOptionalIfNot(groupByColumn.LogicalType());
        if (tableType != groupByType) {
            THROW_ERROR_EXCEPTION(
                "Type mismatch for column %Qv: table has %v, group_by_schema has %v",
                tableColumn.Name(),
                tableType,
                groupByType);
        }
    }

    std::vector<NTableClient::TColumnSchema> rowColumns(
        tableSchema.Columns().begin() + groupBySchema.GetColumnCount(),
        tableSchema.Columns().end());

    std::vector<std::string> secondaryKeyColumns;
    for (const auto& column : rowColumns) {
        if (column.SortOrder().has_value()) {
            secondaryKeyColumns.push_back(std::string(column.Name()));
        }
    }

    if (secondaryKeyColumns.empty()) {
        THROW_ERROR_EXCEPTION("Table has no secondary key columns to multiplex over");
    }

    return {New<NTableClient::TTableSchema>(std::move(rowColumns)), std::move(secondaryKeyColumns)};
}

////////////////////////////////////////////////////////////////////////////////

namespace {

//! Formats a value as a SQL literal suitable for a SELECT WHERE clause.
std::string FormatValueLiteral(
    const NTableClient::TUnversionedValue& value,
    NTableClient::EValueType columnType)
{
    using NTableClient::EValueType;
    switch (columnType) {
        case EValueType::Int64:
            return Format("%v", value.Data.Int64);
        case EValueType::Uint64:
            return Format("%vu", value.Data.Uint64);
        case EValueType::Double:
            return Format("%v", value.Data.Double);
        case EValueType::Boolean:
            return value.Data.Boolean ? "true" : "false";
        case EValueType::String:
            return Format("%Qv", value.AsStringBuf());
        case EValueType::Any:
            // Any values carry their YSON-text bytes in the value's data buffer.
            return Format("yson_string_to_any(%Qv)", value.AsStringBuf());
        default:
            THROW_ERROR_EXCEPTION("Unsupported column type for SELECT literal: %v", columnType);
    }
}

std::string BuildColumnList(const std::vector<std::string>& columns)
{
    return Format("(%v)", JoinSeq(",", columns));
}

std::string BuildLiteralTuple(
    const TKey& key,
    const NTableClient::TTableSchema& keySchema)
{
    YT_VERIFY(key.Underlying().GetCount() == keySchema.GetColumnCount());
    std::vector<std::string> literals;
    literals.reserve(keySchema.GetColumnCount());
    for (int i = 0; i < keySchema.GetColumnCount(); ++i) {
        literals.push_back(FormatValueLiteral(
            key.Underlying()[i],
            keySchema.Columns()[i].GetWireType()));
    }
    return Format("(%v)", JoinSeq(",", literals));
}

std::string BuildComparisonClause(
    const std::vector<std::string>& columnNames,
    TStringBuf op,
    const TKey& key,
    const NTableClient::TTableSchema& keySchema)
{
    return Format("%v %v %v",
        BuildColumnList(columnNames),
        op,
        BuildLiteralTuple(key, keySchema));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TParameterizedSelectQuery BuildSelectQuery(
    const NTableClient::TTableSchema& groupBySchema,
    const TKey& key,
    const std::vector<std::string>& secondaryKeyColumns,
    const std::optional<TKey>& startOffsetExclusive,
    const std::optional<TKey>& endOffsetInclusive,
    const NTableClient::TTableSchema& rowSchema,
    const NYPath::TYPath& tablePath,
    i64 limit,
    const std::string& additionalWhere)
{
    std::vector<std::string> groupByColumnNames;
    groupByColumnNames.reserve(groupBySchema.Columns().size());
    for (const auto& column : groupBySchema.Columns()) {
        groupByColumnNames.push_back(std::string(column.Name()));
    }

    // The first secondaryKeyColumns.size() columns of rowSchema are exactly
    // the secondary key columns (in the same order).
    YT_VERIFY(rowSchema.GetColumnCount() >= std::ssize(secondaryKeyColumns));
    auto secondaryKeySchema = New<NTableClient::TTableSchema>(std::vector(
        rowSchema.Columns().begin(),
        rowSchema.Columns().begin() + std::ssize(secondaryKeyColumns)));

    std::vector<std::string> selectColumns;
    selectColumns.reserve(rowSchema.Columns().size());
    for (const auto& column : rowSchema.Columns()) {
        selectColumns.push_back(std::string(column.Name()));
    }

    std::vector<std::string> whereClauses;
    whereClauses.push_back(BuildComparisonClause(groupByColumnNames, "=", key, groupBySchema));
    if (startOffsetExclusive) {
        whereClauses.push_back(BuildComparisonClause(
            secondaryKeyColumns,
            ">",
            *startOffsetExclusive,
            *secondaryKeySchema));
    }
    if (endOffsetInclusive) {
        whereClauses.push_back(BuildComparisonClause(
            secondaryKeyColumns,
            "<=",
            *endOffsetInclusive,
            *secondaryKeySchema));
    }
    if (!additionalWhere.empty()) {
        whereClauses.push_back(Format("(%v)", additionalWhere));
    }

    // YT requires ORDER BY to use the full table sort-key prefix.
    std::vector<std::string> orderByColumns = groupByColumnNames;
    orderByColumns.insert(orderByColumns.end(), secondaryKeyColumns.begin(), secondaryKeyColumns.end());

    auto query = Format("%v FROM [%v] WHERE %v ORDER BY %v LIMIT %v",
        JoinSeq(",", selectColumns),
        tablePath,
        JoinSeq(" AND ", whereClauses),
        JoinSeq(",", orderByColumns),
        limit);

    return TParameterizedSelectQuery{
        .Query = std::move(query),
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDynamicTableMultiplexer

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
