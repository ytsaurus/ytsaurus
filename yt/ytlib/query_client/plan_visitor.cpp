#include "stdafx.h"
#include "plan_node.h"
#include "plan_visitor.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

static const int TypicalQueueLength = 16;

////////////////////////////////////////////////////////////////////////////////

#define IMPLEMENT_VISITOR(visitorType, nodeType, nodeKind) \
bool visitorType::Visit(const T##nodeKind##nodeType *) \
{ return true; }
#define XX(kind) IMPLEMENT_VISITOR(TPlanVisitor, Operator, kind)
#include "list_of_operators.inc"
#undef XX
#define XX(kind) IMPLEMENT_VISITOR(TPlanVisitor, Expression, kind)
#include "list_of_expressions.inc"
#undef XX
#undef IMPLEMENT_VISITOR

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
            case EExpressionKind::Literal: {
                auto* typedItem = item->As<TLiteralExpression>();
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
        }
    }

    return true;
}

const TOperator* Apply(
    TPlanContext* context,
    const TOperator* root,
    const std::function<const TOperator*(TPlanContext*, const TOperator*)>& functor)
{
    // TODO(sandello): Implement COW to get rid of extra copies.
    auto* mutatedRoot = functor(context, root)->Clone(context);
    switch (mutatedRoot->GetKind()) {
        case EOperatorKind::Scan:
            break;
        case EOperatorKind::Filter: {
            auto* typedMutatedRoot = mutatedRoot->As<TFilterOperator>();
            typedMutatedRoot->SetSource(Apply(context, typedMutatedRoot->GetSource(), functor));
            break;
        }
        case EOperatorKind::Group: {
            auto* typedMutatedRoot = mutatedRoot->As<TGroupOperator>();
            typedMutatedRoot->SetSource(Apply(context, typedMutatedRoot->GetSource(), functor));
            break;
        }
        case EOperatorKind::Project:{
            auto* typedMutatedRoot = mutatedRoot->As<TProjectOperator>();
            typedMutatedRoot->SetSource(Apply(context, typedMutatedRoot->GetSource(), functor));
            break;
        }
    }
    return mutatedRoot;
}

const TExpression* Apply(
    TPlanContext* context,
    const TExpression* root,
    const std::function<const TExpression*(TPlanContext*, const TExpression*)>& functor)
{
    // TODO(sandello): Implement COW to get rid of extra copies.
    auto* mutatedRoot = functor(context, root)->Clone(context);
    switch (mutatedRoot->GetKind()) {
        case EExpressionKind::Literal:
        case EExpressionKind::Reference:
            break;
        case EExpressionKind::Function: {
            auto* typedMutatedRoot = mutatedRoot->As<TFunctionExpression>();
            for (auto& argument : typedMutatedRoot->Arguments()) {
                argument = Apply(context, argument, functor);
            }
            break;
        }
        case EExpressionKind::BinaryOp: {
            auto* typedMutatedRoot = mutatedRoot->As<TBinaryOpExpression>();
            typedMutatedRoot->SetLhs(Apply(context, typedMutatedRoot->GetLhs(), functor));
            typedMutatedRoot->SetRhs(Apply(context, typedMutatedRoot->GetRhs(), functor));
            break;
        }
    }
    return mutatedRoot;
}

void Visit(
    const TExpression* root,
    const std::function<void(const TExpression*)>& visitor)
{
    visitor(root);
    switch (root->GetKind()) {
        case EExpressionKind::Literal:
        case EExpressionKind::Reference:
            break;
        case EExpressionKind::Function:
            for (const auto& argument : root->As<TFunctionExpression>()->Arguments()) {
                Visit(argument, visitor);
            }
            break;
        case EExpressionKind::BinaryOp:
            Visit(root->As<TBinaryOpExpression>()->GetLhs(), visitor);
            Visit(root->As<TBinaryOpExpression>()->GetRhs(), visitor);
            break;
    }
}

void Visit(
    const TOperator* root,
    const std::function<void(const TOperator*)>& visitor)
{
    visitor(root);
    switch (root->GetKind()) {
        case EOperatorKind::Scan:
            break;
        case EOperatorKind::Filter:
            Visit(root->As<TFilterOperator>()->GetSource(), visitor);
            break;
        case EOperatorKind::Group:
            Visit(root->As<TGroupOperator>()->GetSource(), visitor);
            break;
        case EOperatorKind::Project:
            Visit(root->As<TProjectOperator>()->GetSource(), visitor);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

