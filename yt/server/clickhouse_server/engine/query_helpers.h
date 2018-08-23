#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

DB::ASTTableExpression* GetFirstTableExpression(DB::ASTSelectQuery& select);

} // namespace NClickHouse
} // namespace NYT
