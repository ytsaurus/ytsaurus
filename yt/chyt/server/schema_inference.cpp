#include "schema_inference.h"

#include "config.h"
#include "table.h"

#include <yt/yt/client/complex_types/check_type_compatibility.h>

namespace NYT::NClickHouseServer {

using namespace NComplexTypes;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

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
    // Sometimes types are similar, but they could not be converted from one another.
    // E.g. types optional<i32> and i64 are not compatible directly, but we can deduce
    // type optional<i64> if we handle isOptional and underlying type separately.
    // For these cases we store common type and its isOptional separately.
    // There still can be some cases where this heuristic does not work,
    // e.g. for types list<optional<i32>> and list<i64>, but it's difficult to
    // handle all these cases and probably no one really needs this.
    TLogicalTypePtr commonType;
    bool isOptional = false;
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
            commonType = RemoveOptional(column.LogicalType());
            isOptional = !column.Required();
            commonSortOrder = column.SortOrder();
            tableIndexWithColumn = tableIndex;
            break;
        }
    }

    YT_VERIFY(tableIndexWithColumn != -1);

    auto onColumnMiss = [&] (int tableIndex) {
        switch (settings->MissingColumnMode) {
            case EMissingColumnMode::Throw: {
                THROW_ERROR_EXCEPTION("Column %v is missing in input table %Qv but is present in %Qv; "
                    "you can specify how to handle missing columns via setting chyt.concat_tables.missing_column_mode",
                    columnName,
                    tables[tableIndex]->Path,
                    tables[tableIndexWithColumn]->Path)
                    << TErrorAttribute("docs", "https://yt.yandex-team.ru/docs/description/chyt/reference/settings");
            }
            case EMissingColumnMode::Drop: {
                shouldDropColumn = true;
                break;
            }
            case EMissingColumnMode::ReadAsNull: {
                isOptional = true;
                commonSortOrder = std::nullopt;
                break;
            }
        }
    };

    // First pass.
    // Try to find 'the most common' type among all column types and process missing columns.
    // We can not handle type mismatch on the first pass since we can find column
    // with 'Any' type later.
    for (int tableIndex = 0; tableIndex < tableCount; ++tableIndex) {
        const auto& tableSchema = tables[tableIndex]->Schema;
        int columnIndex = columnIndices[tableIndex];

        if (columnIndex == -1) {
            isOptional = true;
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
            auto columnType = RemoveOptional(column.LogicalType());

            if (!column.Required()) {
                isOptional = true;
            }

            // Update commonType if current column type is more general (e.g. i32 -> i64).
            // We will handle incompatible types (e.g. i32 and String) later.
            auto [compitability, _] = CheckTypeCompatibility(commonType, columnType);
            if (compitability == ESchemaCompatibility::FullyCompatible) {
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
                    << TErrorAttribute("docs", "https://yt.yandex-team.ru/docs/description/chyt/reference/settings");
            }
            case ETypeMismatchMode::Drop: {
                shouldDropColumn = true;
                break;
            }
            case ETypeMismatchMode::ReadAsAny: {
                commonType = SimpleLogicalType(ESimpleLogicalValueType::Any);
                // Type 'Any' can not be required.
                isOptional = true;
                // Type 'Any' is not comparable.
                commonSortOrder = std::nullopt;
                break;
            }
        }
    };

    // Second pass.
    // We have found 'the most common' type, but it can be incompatible with some columns.
    // Validate that all types are compatibile with 'the most common' one.
    for (int tableIndex = 0; tableIndex < tableCount; ++tableIndex) {
        const auto& tableSchema = tables[tableIndex]->Schema;
        int columnIndex = columnIndices[tableIndex];

        if (columnIndex == -1) {
            // Missed columns in non-strict schema can be read only as Any, because
            // they can actually be present in chunks with any type.
            if (!tableSchema->GetStrict() && *commonType != *SimpleLogicalType(ESimpleLogicalValueType::Any)) {
                onTypeMismatch(
                    TError("The column %v is missing in input table %Qv with non-strict schema; "
                        "read of the column is forbidden because it may be present in data with a mismatched type; "
                        "you can specify how to handle incompatible column types "
                        "via setting chyt.concat_tables.type_mismatch_mode",
                        columnName,
                        tables[tableIndex]->Path));
            }
        } else {
            const auto& column = tableSchema->Columns()[columnIndex];
            auto columnType = RemoveOptional(column.LogicalType());

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

    if (isOptional) {
        commonType = OptionalLogicalType(std::move(commonType));
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

    std::vector<THashMap<TString, int>> columnToPositionMaps;
    columnToPositionMaps.reserve(schemas.size());

    for (const auto& table : uniqueTables) {
        const auto& schema = table->Schema;
        auto& columnToPositionMap = columnToPositionMaps.emplace_back();

        for (int columnIndex = 0; columnIndex < schema->GetColumnCount(); ++columnIndex) {
            const auto& columnName = schema->Columns()[columnIndex].Name();
            auto [_, inserted] = columnToPositionMap.emplace(columnName, columnIndex);
            YT_VERIFY(inserted);
        }

        maxColumnCount = std::max(maxColumnCount, schema->GetColumnCount());
    }

    auto getColumnPositions = [&] (const TString& columnName) {
        std::vector<int> positions;
        positions.reserve(schemas.size());

        int foundInAllSchemas = true;

        for (const auto& columnToPositionMap : columnToPositionMaps) {
            auto positionIt = columnToPositionMap.find(columnName);
            if (positionIt != columnToPositionMap.end()) {
                positions.push_back(positionIt->second);
            } else {
                positions.push_back(-1);
                foundInAllSchemas = false;
            }
        }
        return std::make_pair(std::move(positions), foundInAllSchemas);
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
            << TErrorAttribute("docs", "https://yt.yandex-team.ru/docs/description/chyt/reference/settings");
    }

    return New<TTableSchema>(std::move(commonColumns), strict);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
