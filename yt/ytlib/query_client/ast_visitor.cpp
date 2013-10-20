#include "ast.h"
#include "ast_visitor.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

static const int TypicalQueueLength = 16;

////////////////////////////////////////////////////////////////////////////////

#define XX(nodeType) IMPLEMENT_AST_VISITOR_DUMMY(IExpressionAstVisitor, nodeType)
#include "list_of_operators.inc"
#undef XX

#define XX(nodeType) IMPLEMENT_AST_VISITOR_DUMMY(IOperatorAstVisitor, nodeType)
#include "list_of_expressions.inc"
#undef XX

#define XX(nodeType) IMPLEMENT_AST_VISITOR_DUMMY(TAstVisitor, nodeType)
#include "list_of_operators.inc"
#include "list_of_expressions.inc"
#undef XX

bool Traverse(IAstVisitor* visitor, TOperator* root)
{
    TSmallVector<TOperator*, TypicalQueueLength> queue;
    queue.push_back(root);

    while (!queue.empty()) {
        auto item = queue.pop_back_val();

        if (!item->Accept(visitor)) {
            return false;
        }

        for (const auto& child : item->Children()) {
            queue.push_back(child);
        }
    }

    return true;
}

bool Traverse(IAstVisitor* visitor, TExpression* root)
{
    TSmallVector<TExpression*, TypicalQueueLength> queue;
    queue.push_back(root);

    while (!queue.empty()) {
        auto item = queue.pop_back_val();

        if (!item->Accept(visitor)) {
            return false;
        }

        for (const auto& child : item->Children()) {
            queue.push_back(child);
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

