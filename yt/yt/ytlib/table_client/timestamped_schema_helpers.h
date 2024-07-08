#pragma once

#include "public.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TColumnToTimestampColumn
{
    int ColumnIndex;
    int TimestampColumnIndex;
};

using TTimestampColumnMapping = std::vector<TColumnToTimestampColumn>;

struct TTimestampReadOptions
{
    TTimestampColumnMapping TimestampColumnMapping;
    // Original indexes of columns for which timestamp is requested and value is not.
    // TODO(dave11ar): Read only timestamps without value for such columns.
    std::vector<int> TimestampOnlyColumns;
};

TTableSchemaPtr ToLatestTimestampSchema(const TTableSchemaPtr& schema);

TColumnFilter CreateLatestTimestampColumnFilter(
    const TColumnFilter& columnFilter,
    const TTableSchemaPtr& originalSchema,
    const TTimestampReadOptions& timestampReadOptions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
