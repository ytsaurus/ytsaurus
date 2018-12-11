#pragma once

#include "clickhouse.h"

//#include <Parsers/ASTSelectQuery.h>
//#include <Parsers/ASTTablesInSelectQuery.h>

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

DB::ASTTableExpression* GetFirstTableExpression(DB::ASTSelectQuery& select);

std::vector<DB::ASTTableExpression*> GetAllTableExpressions(DB::ASTSelectQuery& select);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
