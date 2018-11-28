#include "query_helpers.h"

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

ASTTableExpression* GetFirstTableExpression(ASTSelectQuery& select)
{
    if (!select.tables) {
        return nullptr;
    }

    const auto& tablesInSelectQuery = static_cast<const ASTTablesInSelectQuery &>(*select.tables);
    if (tablesInSelectQuery.children.size() != 1) {
        return nullptr;
    }

    const auto& tablesElement = static_cast<const ASTTablesInSelectQueryElement &>(*tablesInSelectQuery.children[0]);
    if (!tablesElement.table_expression) {
        return nullptr;
    }

    return static_cast<ASTTableExpression *>(tablesElement.table_expression.get());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
