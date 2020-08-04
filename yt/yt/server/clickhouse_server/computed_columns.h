#pragma once

#include "private.h"

#include <yt/client/table_client/schema.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::ASTPtr PopulatePredicateWithComputedColumns(
    DB::ASTPtr ast,
    const NTableClient::TTableSchemaPtr& schema,
    const DB::Context& context,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
