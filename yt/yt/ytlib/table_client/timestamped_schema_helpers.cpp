#include "timestamped_schema_helpers.h"

#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr ToLatestTimestampSchema(const TTableSchemaPtr& schema)
{
    std::vector<TColumnSchema> columns;
    columns.reserve(schema->GetColumnCount() + schema->GetValueColumnCount());

    for (int columnIndex = 0; columnIndex < schema->GetColumnCount(); ++columnIndex) {
        columns.push_back(schema->Columns()[columnIndex]);
    }
    for (int columnIndex = schema->GetKeyColumnCount(); columnIndex < schema->GetColumnCount(); ++columnIndex) {
        columns.emplace_back(TimestampColumnPrefix + schema->Columns()[columnIndex].Name(), EValueType::Uint64);
    }

    return New<TTableSchema>(
        std::move(columns),
        schema->GetStrict(),
        schema->GetUniqueKeys(),
        schema->GetSchemaModification(),
        schema->DeletedColumns());
}

TColumnFilter CreateLatestTimestampColumnFilter(
    const TColumnFilter& columnFilter,
    const TTableSchemaPtr& originalSchema,
    const TTimestampReadOptions& timestampReadOptions)
{
    YT_ASSERT(!timestampReadOptions.TimestampColumnMapping.empty());

    TColumnFilter::TIndexes indexes;

    std::vector<int> timestampOnlyColumns = timestampReadOptions.TimestampOnlyColumns;
    std::sort(timestampOnlyColumns.begin(), timestampOnlyColumns.end());

    auto addIndex = [&] (int columnIndex) {
        if (!std::binary_search(timestampOnlyColumns.begin(), timestampOnlyColumns.end(), columnIndex)) {
            indexes.push_back(columnIndex);
        }
    };

    if (columnFilter.IsUniversal()) {
        for (int columnIndex = 0; columnIndex < originalSchema->GetColumnCount(); ++columnIndex) {
            addIndex(columnIndex);
        }
    } else {
        for (int columnIndex : columnFilter.GetIndexes()) {
            addIndex(columnIndex);
        }
    }

    for (auto [columnIndex, timestampColumnIndex] : timestampReadOptions.TimestampColumnMapping) {
        indexes.push_back(timestampColumnIndex);
    }

    return TColumnFilter(std::move(indexes));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
