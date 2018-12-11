#include "query_helpers.h"

//#include <Parsers/ASTFunction.h>
//#include <Parsers/ASTLiteral.h>

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

std::vector<ASTTableExpression*> GetAllTableExpressions(ASTSelectQuery& select)
{
    if (!select.tables) {
        return {};
    }

    std::vector<ASTTableExpression*> result;

    const auto& tablesInSelectQuery = static_cast<const ASTTablesInSelectQuery &>(*select.tables);
    for (const auto& tableInSelectQuery : tablesInSelectQuery.children) {
        const auto& tablesElement = static_cast<const ASTTablesInSelectQueryElement &>(*tableInSelectQuery);
        if (!tablesElement.table_expression) {
            continue;
        }

        result.emplace_back(static_cast<ASTTableExpression *>(tablesElement.table_expression.get()));
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
