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

#ifdef __GNUC__ 
#pragma GCC diagnostic push
#pragma GCC diagnostic error "-Wswitch-enum"
#endif

bool Traverse(IAstVisitor* visitor, const TOperator* root)
{
    TSmallVector<const TOperator*, TypicalQueueLength> queue;
    queue.push_back(root);

    while (!queue.empty()) {
        auto* item = queue.pop_back_val();
        switch (item->GetKind()) {
            case EOperatorKind::Scan: {
                auto* typedItem = item->As<TScanOperator>();
                if (!visitor->Visit(typedItem)) { return false; }
                break;
            }
            case EOperatorKind::Union: {
                auto* typedItem = item->As<TUnionOperator>();
                if (!visitor->Visit(typedItem)) { return false; }
                queue.append(
                    typedItem->Sources().begin(),
                    typedItem->Sources().end());
                break;
            }
            case EOperatorKind::Filter: {
                auto* typedItem = item->As<TFilterOperator>();
                if (!visitor->Visit(typedItem)) { return false; }
                queue.push_back(typedItem->GetSource());
                break;
            }
            case EOperatorKind::Project: {
                auto* typedItem = item->As<TProjectOperator>();
                if (!visitor->Visit(typedItem)) { return false; }
                queue.push_back(typedItem->GetSource());
                break;
            }
            default:
                YUNREACHABLE();
        }
    }

    return true;
}

bool Traverse(IAstVisitor* visitor, const TExpression* root)
{
    TSmallVector<const TExpression*, TypicalQueueLength> queue;
    queue.push_back(root);

    while (!queue.empty()) {
        auto* item = queue.pop_back_val();
        switch (item->GetKind()) {
            case EExpressionKind::IntegerLiteral: {
                auto* typedItem = item->As<TIntegerLiteralExpression>();
                if (!visitor->Visit(typedItem)) { return false; }
                break;
            }
            case EExpressionKind::DoubleLiteral: {
                auto* typedItem = item->As<TDoubleLiteralExpression>();
                if (!visitor->Visit(typedItem)) { return false; }
                break;
            }
            case EExpressionKind::Reference: {
                auto* typedItem = item->As<TReferenceExpression>();
                if (!visitor->Visit(typedItem)) { return false; }
                break;
            }
            case EExpressionKind::Function: {
                auto* typedItem = item->As<TFunctionExpression>();
                if (!visitor->Visit(typedItem)) { return false; }
                queue.append(
                    typedItem->Arguments().begin(),
                    typedItem->Arguments().end());
                break;
            }
            case EExpressionKind::BinaryOp: {
                auto* typedItem = item->As<TBinaryOpExpression>();
                if (!visitor->Visit(typedItem)) { return false; }
                queue.push_back(typedItem->GetLhs());
                queue.push_back(typedItem->GetRhs());
                break;
            }
            default:
                YUNREACHABLE();
        }
    }

    return true;
}

#ifdef __GNUC__ 
#pragma GCC diagnostic pop
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

