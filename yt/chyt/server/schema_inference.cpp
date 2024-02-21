#include "schema_inference.h"

#include "config.h"
#include "table.h"

#include <yt/yt/client/table_client/check_schema_compatibility.h>
#include <yt/yt/client/table_client/logical_type.h>

#include <yt/yt/client/complex_types/check_type_compatibility.h>

namespace NYT::NClickHouseServer {

using namespace NComplexTypes;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TLogicalTypePtr MakeNullableIfNot(TLogicalTypePtr element) {
    if (element->IsNullable()) {
        return element;
    }
    return OptionalLogicalType(std::move(element));
}

std::optional<TColumnSchema> InferCommonColumnSchema(
    const std::vector<int>& columnIndices,
    const std::vector<TTablePtr>& tables,
    bool keepSortOrder,
    const TConcatTablesSettingsPtr& settings)
{
    YT_VERIFY(columnIndices.size() == tables.size());
    YT_VERIFY(!tables.empty());

    int tableCount = tables.size();

    TString columnName;

    TLogicalTypePtr commonType;
    std::optional<ESortOrder> commonSortOrder;

    bool shouldDropColumn = false;

    // Table index with present column for generating pretty errors.
    int tableIndexWithColumn = -1;

    // Find first present column before any checks for generating proper errors.
    for (int tableIndex = 0; tableIndex < tableCount; ++tableIndex) {
        int columnIndex = columnIndices[tableIndex];
        if (columnIndex != -1) {
            const auto& column = tables[tableIndex]->Schema->Columns()[columnIndex];
            columnName = column.Name();
            commonType = column.LogicalType();
            commonSortOrder = column.SortOrder();
            tableIndexWithColumn = tableIndex;
            break;
        }
    }

    YT_VERIFY(tableIndexWithColumn != -1);

    auto onColumnMiss = [&] (int tableIndex) {
        switch (settings->MissingColumnMode) {
            case EMissingColumnMode::Throw: {
                THROW_ERROR_EXCEPTION("Column %Qv is missing in input table %v but is present in %v; "
                    "you can specify how to handle missing columns via setting chyt.concat_tables.missing_column_mode",
                    columnName,
                    tables[tableIndex]->Path,
                    tables[tableIndexWithColumn]->Path)
                    << TErrorAttribute("docs", "https://ytsaurus.tech/docs/en/user-guide/data-processing/chyt/reference/settings");
            }
            case EMissingColumnMode::Drop: {
                shouldDropColumn = true;
                break;
            }
            case EMissingColumnMode::ReadAsNull: {
                commonType = MakeNullableIfNot(std::move(commonType));
                commonSortOrder = std::nullopt;
                break;
            }
        }
    };

    // First pass.
    // Try to find 'the most common' type among all column types and process missing columns.
    // We can not handle type mismatch on the first pass since we do not know the common type yet.
    for (int tableIndex = 0; tableIndex < tableCount; ++tableIndex) {
        const auto& tableSchema = tables[tableIndex]->Schema;
        int columnIndex = columnIndices[tableIndex];

        if (columnIndex == -1) {
            commonType = MakeNullableIfNot(std::move(commonType));
            commonSortOrder = std::nullopt;
            // If column is missing in non-strict schema, it may still be present in chunks.
            // We pessimistically assume that the column is present in such tables and
            // there is a type mismatch, so column can be read only as Nullable(Any).
            // We will handle type mismatch later, now we only handle missing columns in
            // strict schemas.
            if (tableSchema->GetStrict()) {
                onColumnMiss(tableIndex);
            }
        } else {
            const auto& column = tableSchema->Columns()[columnIndex];
            auto columnType = column.LogicalType();

            // Common type should be nullable if at least one column is nullable/missing.
            if (columnType->IsNullable()) {
                commonType = MakeNullableIfNot(std::move(commonType));
            }

            // Making columnType Nullable helps to handle some cases when types are incompatible,
            // but despite this they have a supertype (e.g. int64 and optional<int32>, supertype is optional<int64>).
            // TODO(dakovalkov): This is not a silver bullet and it will not work in more complex cases (e.g. list<int64> and list<optional<32>>)
            // For proper type handing we need a GetLeastSupertype function, but its implementations is not trivial.
            if (commonType->IsNullable()) {
                columnType = MakeNullableIfNot(std::move(columnType));
            }

            // Update commonType if current column type is more general (e.g. i32 -> i64).
            // We will handle incompatible types (e.g. i32 and String) later.
            auto [compatibility, _] = CheckTypeCompatibility(commonType, columnType);
            if (compatibility == ESchemaCompatibility::FullyCompatible) {
                commonType = std::move(columnType);
                tableIndexWithColumn = tableIndex;
            }

            if (columnIndex != columnIndices.front() || commonSortOrder != column.SortOrder()) {
                commonSortOrder = std::nullopt;
            }
        }
    }

    auto onTypeMismatch = [&] (TError error) {
        switch (settings->TypeMismatchMode) {
            case ETypeMismatchMode::Throw: {
                THROW_ERROR_EXCEPTION(std::move(error))
                    << TErrorAttribute("docs", "https://ytsaurus.tech/docs/en/user-guide/data-processing/chyt/reference/settings");
            }
            case ETypeMismatchMode::Drop: {
                shouldDropColumn = true;
                break;
            }
            case ETypeMismatchMode::ReadAsAny: {
                commonType = OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any));
                // We represent Any as yson-string in CH, so sort order is broken after this transform.
                commonSortOrder = std::nullopt;
                break;
            }
        }
    };

    // Second pass.
    // We have found 'the most common' type, but it can be incompatible with some columns.
    // Validate that all types are compatible with 'the most common' one.
    for (int tableIndex = 0; tableIndex < tableCount; ++tableIndex) {
        const auto& tableSchema = tables[tableIndex]->Schema;
        int columnIndex = columnIndices[tableIndex];

        if (columnIndex == -1) {
            // Missed columns in non-strict schema can be read only as Any, because
            // they can actually be present in chunks with any type.
            if (!tableSchema->GetStrict() && *commonType != *OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Any))) {
                onTypeMismatch(
                    TError("Column %Qv is missing in input table %v with non-strict schema; "
                        "read of the column is forbidden because it may be present in data with a mismatched type; "
                        "you can specify how to handle incompatible column types "
                        "via setting chyt.concat_tables.type_mismatch_mode",
                        columnName,
                        tables[tableIndex]->Path));
            }
        } else {
            const auto& column = tableSchema->Columns()[columnIndex];
            auto columnType = column.LogicalType();

            // Check that all columns can be read as 'the most common' type.
            auto [compatibility, _] = CheckTypeCompatibility(columnType, commonType);

            if (compatibility != ESchemaCompatibility::FullyCompatible) {
                onTypeMismatch(
                    TError("The column %v has incompatible types %v and %v in input tables %Qv and %Qv; "
                        "you can specify how to handle incompatible column types "
                        "via setting chyt.concat_tables.type_mismatch_mode",
                        columnName,
                        *columnType,
                        *commonType,
                        tables[tableIndex]->Path,
                        tables[tableIndexWithColumn]->Path));
            }
        }
    }

    if (shouldDropColumn) {
        return std::nullopt;
    }


    if (!keepSortOrder || !IsComparable(commonType)) {
        commonSortOrder = std::nullopt;
    }

    return TColumnSchema(std::move(columnName), std::move(commonType), commonSortOrder);
}

TTableSchemaPtr InferCommonTableSchema(
    const std::vector<TTablePtr>& tables,
    const TConcatTablesSettingsPtr& settings)
{
    YT_VERIFY(!tables.empty());

    using TSchemaHashSet = THashSet<
        NTableClient::TTableSchemaPtr,
        NTableClient::TTableSchemaHash,
        NTableClient::TTableSchemaEquals>;

    // TODO(dakovalkov): Use schema_id to unify schemas when it's available everywhere.
    TSchemaHashSet schemas;

    // Only tables with unique schemas.
    std::vector<TTablePtr> uniqueTables;
    uniqueTables.reserve(tables.size());

    for (const auto& table : tables) {
        auto [_, inserted] = schemas.insert(table->Schema);
        if (inserted) {
            uniqueTables.push_back(table);
        }
    }

    YT_VERIFY(!uniqueTables.empty());

    if (uniqueTables.size() == 1) {
        return uniqueTables.front()->Schema;
    }

    int maxColumnCount = 0;
    for (const auto& table : uniqueTables) {
        maxColumnCount = std::max(maxColumnCount, table->Schema->GetColumnCount());
    }

    auto getColumnPositions = [&] (const TString& columnName) {
        std::vector<int> positions;
        positions.reserve(schemas.size());

        int foundInAllSchemas = true;

        std::optional<TColumnStableName> stableName;
        for (const auto& table : uniqueTables) {
            if (auto* column = table->Schema->FindColumn(columnName)) {
                if (stableName) {
                    if (*stableName != column->StableName()) {
                        THROW_ERROR_EXCEPTION(
                            "Input table schemas have column with same name %Qv "
                            "but different stable names %Qv and %Qv",
                            columnName,
                            *stableName,
                            column->StableName());
                    }
                } else {
                    stableName = column->StableName();
                }
                positions.push_back(table->Schema->GetColumnIndex(*column));
            } else {
                positions.push_back(-1);
                foundInAllSchemas = false;
            }
        }
        return std::pair(std::move(positions), foundInAllSchemas);
    };

    std::vector<TColumnSchema> commonColumns;
    commonColumns.reserve(maxColumnCount);

    bool strict = true;

    THashSet<TString> usedColumnNames;

    bool isSortedPrefix = true;
    int commonColumnInAllSchemasCount = 0;

    for (const auto& table : uniqueTables) {
        const auto& schema = table->Schema;

        for (const auto& column : schema->Columns()) {
            auto [_, inserted] = usedColumnNames.insert(column.Name());
            if (!inserted) {
                continue;
            }

            auto [positions, foundInAllSchemas] = getColumnPositions(column.Name());
            auto commonColumn = InferCommonColumnSchema(
                positions,
                uniqueTables,
                isSortedPrefix,
                settings);

            if (commonColumn) {
                if (!commonColumn->SortOrder()) {
                    isSortedPrefix = false;
                }
                if (foundInAllSchemas) {
                    ++commonColumnInAllSchemasCount;
                }
                commonColumns.emplace_back(std::move(*commonColumn));
            } else {
                // Column is dropped.
                isSortedPrefix = false;
                strict = false;
            }
        }
        // All sorted columns are handled in first schema.
        isSortedPrefix = false;
        // If at least one input schema is not strict,
        // the common schema is not strict too.
        if (!schema->GetStrict()) {
            strict = false;
        }
    }

    if (commonColumnInAllSchemasCount == 0 && !settings->AllowEmptySchemaIntersection) {
        THROW_ERROR_EXCEPTION("Input table schemas have no common columns in intersection; "
            "probably there are tables with wrong or empty schema among concatenated tables; "
            "if it is not a mistake, you can disable this check "
            "via setting chyt.concat_tables.allow_empty_schema_intersection")
            << TErrorAttribute("docs", "https://ytsaurus.tech/docs/en/user-guide/data-processing/chyt/reference/settings");
    }

    auto commonSchema = New<TTableSchema>(std::move(commonColumns), strict);

    // NOTE(dakovalkov): This sanity check fails because CheckTableSchemaCompatibility denies
    // to drop composite columns. This restriction can be valid for chunk teleportation,
    // but it is redundant for CHYT.

    // // Sanity check. Validate that all tables can be read through commonSchema.
    // for (const auto& table : uniqueTables) {
    //     auto [compatibility, error] = CheckTableSchemaCompatibility(
    //         *table->Schema,
    //         *commonSchema,
    //         /*ignoreSortOrder*/ false);

    //     if (compatibility != ESchemaCompatibility::FullyCompatible) {
    //         THROW_ERROR_EXCEPTION("Common schema validation failed; it is a bug; "
    //             "please, contanct the developers and provide the query and full error message")
    //             << TErrorAttribute("common_schema", commonSchema)
    //             << TErrorAttribute("input_table", table->Path)
    //             << TErrorAttribute("input_schema", table->Schema)
    //             << error;
    //     }
    // }

    return commonSchema;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
