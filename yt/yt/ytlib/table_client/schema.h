#pragma once

#include <yt/yt/client/table_client/public.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr InferInputSchema(
    const std::vector<TTableSchemaPtr>& schemas,
    bool discardKeyColumns);

////////////////////////////////////////////////////////////////////////////////

void ValidateIndexSchema(
    const TTableSchema& tableSchema,
    const TTableSchema& indexTableSchema,
    const TColumnSchema** unfoldedColumn = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
