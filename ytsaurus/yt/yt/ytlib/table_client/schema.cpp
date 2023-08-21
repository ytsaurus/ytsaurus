#include "schema.h"

// XXX(max42): this is a workaround for some weird linkage error.
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/column_sort_schema.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr InferInputSchema(const std::vector<TTableSchemaPtr>& schemas, bool discardKeyColumns)
{
    YT_VERIFY(!schemas.empty());

    // NB: If one schema is not strict then the resulting schema should be an intersection, not union.
    for (const auto& schema : schemas) {
        if (!schema->GetStrict()) {
            THROW_ERROR_EXCEPTION("Input table schema is not strict");
        }
    }

    int commonKeyColumnPrefix = 0;
    if (!discardKeyColumns) {
        std::vector<TSortColumns> allSortColumns;
        for (const auto& schema : schemas) {
            allSortColumns.push_back(schema->GetSortColumns());
        }
        for (; commonKeyColumnPrefix < ssize(allSortColumns.front()); ++commonKeyColumnPrefix) {
            const auto& firstSortColumn = allSortColumns.front()[commonKeyColumnPrefix];
            auto match = std::all_of(begin(allSortColumns), end(allSortColumns), [&] (const TSortColumns& sortColumns) {
                return commonKeyColumnPrefix < ssize(sortColumns) && firstSortColumn == sortColumns[commonKeyColumnPrefix];
            });
            if (!match) {
                break;
            }
        }
    }

    std::vector<TColumnSchema> columns;
    THashMap<TStableName, int> stableNameToColumnIndex;
    THashMap<TString, int> nameToColumnIndex;
    for (const auto& schema : schemas) {
        for (int columnIndex = 0; columnIndex < schema->GetColumnCount(); ++columnIndex) {
            auto column = schema->Columns()[columnIndex];
            if (columnIndex >= commonKeyColumnPrefix) {
                column.SetSortOrder(std::nullopt);
            }
            column
                .SetExpression(std::nullopt)
                .SetAggregate(std::nullopt)
                .SetLock(std::nullopt);

            auto it = nameToColumnIndex.find(column.Name());
            if (it == nameToColumnIndex.end()) {
                columns.push_back(column);
                auto index = ssize(columns) - 1;
                EmplaceOrCrash(nameToColumnIndex, column.Name(), index);
                if (auto [it, inserted] = stableNameToColumnIndex.emplace(column.StableName(), index); !inserted) {
                    THROW_ERROR_EXCEPTION(
                        "Conflict while merging schemas: duplicate stable name %Qv for columns with differing names",
                        column.StableName().Get())
                        << TErrorAttribute("first_column_schema", columns[it->second])
                        << TErrorAttribute("second_column_schema", columns[index]);
                }
            } else {
                if (columns[it->second] != column) {
                    THROW_ERROR_EXCEPTION(
                        "Conflict while merging schemas: column %v has two conflicting declarations",
                        column.GetDiagnosticNameString())
                        << TErrorAttribute("first_column_schema", columns[it->second])
                        << TErrorAttribute("second_column_schema", column);
                }
            }
        }
    }

    return New<TTableSchema>(std::move(columns));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
