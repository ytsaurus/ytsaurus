#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

DB::ASTTableExpression* GetFirstTableExpression(DB::ASTSelectQuery& select);

std::vector<DB::ASTTableExpression*> GetAllTableExpressions(DB::ASTSelectQuery& select);

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
