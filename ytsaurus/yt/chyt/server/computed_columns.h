#pragma once

#include "private.h"

#include <yt/yt/client/table_client/schema.h>

#include <Interpreters/PreparedSets.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::ASTPtr PopulatePredicateWithComputedColumns(
    DB::ASTPtr ast,
    const NTableClient::TTableSchemaPtr& schema,
    DB::ContextPtr context,
    DB::PreparedSets& preparedSets,
    const TQuerySettingsPtr& settings,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
