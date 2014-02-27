#include "plan_node.h"
#include "plan_visitor.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

static const int TypicalQueueLength = 16;

////////////////////////////////////////////////////////////////////////////////

#define XX(nodeType) IMPLEMENT_AST_VISITOR_DUMMY(IExpressionVisitor, nodeType)
#include "list_of_operators.inc"
#undef XX

#define XX(nodeType) IMPLEMENT_AST_VISITOR_DUMMY(IOperatorVisitor, nodeType)
#include "list_of_expressions.inc"
#undef XX

#define XX(nodeType) IMPLEMENT_AST_VISITOR_DUMMY(TPlanVisitor, nodeType)
#include "list_of_operators.inc"
#include "list_of_expressions.inc"
#undef XX

#ifdef __GNUC__
#pragma GCC diagnostic push
#pragma GCC diagnostic error "-Wswitch-enum"
#endif

bool Traverse(IPlanVisitor* visitor, const TOperator* root)
{
    SmallVector<const TOperator*, TypicalQueueLength> queue;
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
            case EOperatorKind::Group: {
                auto* typedItem = item->As<TGroupOperator>();
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

bool Traverse(IPlanVisitor* visitor, const TExpression* root)
{
    SmallVector<const TExpression*, TypicalQueueLength> queue;
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

template <class TNode>
static inline const TNode* ApplyImpl(
    TPlanContext* context,
    const TNode* root,
    const std::function<const TNode*(TPlanContext*, const TNode*)>& functor)
{
    const TNode* result = functor(context, root);

    auto immutableChildren = result->Children();
    auto mutableChildren = TMutableArrayRef<const TNode*>(
        const_cast<const TNode**>(immutableChildren.data()),
        immutableChildren.size());

    for (auto& child : mutableChildren) {
        child = Apply(context, child, functor);
    }

    return result;
}

const TOperator* Apply(
    TPlanContext* context,
    const TOperator* root,
    const std::function<const TOperator*(TPlanContext*, const TOperator*)>& functor)
{
    return ApplyImpl(context, root, functor);
}

const TExpression* Apply(
    TPlanContext* context,
    const TExpression* root,
    const std::function<const TExpression*(TPlanContext*, const TExpression*)>& functor)
{
    return ApplyImpl(context, root, functor);
}

template <class TNode>
static inline void VisitImpl(
    const TNode* root,
    const std::function<void(const TNode*)>& visitor)
{
    visitor(root);
    for (const auto& child : root->Children()) {
        Visit(child, visitor);
    }
}

void Visit(
    const TExpression* root,
    const std::function<void(const TExpression*)>& visitor)
{
    VisitImpl(root, visitor);
}

void Visit(
    const TOperator* root,
    const std::function<void(const TOperator*)>& visitor)
{
    VisitImpl(root, visitor);
}


#ifdef __GNUC__
#pragma GCC diagnostic pop
#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

