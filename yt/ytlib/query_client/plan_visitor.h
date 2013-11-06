#pragma once

#include <core/misc/array_ref.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TOperator;
class TExpression;

class TQueryContext;

#define AS_INTERFACE(nodeType) \
    virtual bool Visit(const T ## nodeType *) = 0;
#define AS_IMPLEMENTATION(nodeType) \
    virtual bool Visit(const T ## nodeType *) override;

struct IPlanVisitor
{
    virtual ~IPlanVisitor()
    { }

#define XX(nodeType) AS_INTERFACE(nodeType)
#include "list_of_expressions.inc"
#include "list_of_operators.inc"
#undef XX
};

struct IExpressionVisitor
    : IPlanVisitor
{
#define XX(nodeType) AS_IMPLEMENTATION(nodeType)
#include "list_of_operators.inc"
#undef XX
};

struct IOperatorVisitor
    : IPlanVisitor
{
#define XX(nodeType) AS_IMPLEMENTATION(nodeType)
#include "list_of_expressions.inc"
#undef XX
};

class TPlanVisitor
    : public IPlanVisitor
{
// Stub out everything.
#define XX(nodeType) AS_IMPLEMENTATION(nodeType)
#include "list_of_expressions.inc"
#include "list_of_operators.inc"
#undef XX
};

#undef AS_INTERFACE
#undef AS_IMPLEMENTATION

#define IMPLEMENT_AST_VISITOR_DUMMY(visitorType, nodeType) \
bool visitorType::Visit(const T ## nodeType*) \
{ return true; }

//! Runs a depth-first preorder traversal.
bool Traverse(IPlanVisitor* visitor, const TOperator* root);
bool Traverse(IPlanVisitor* visitor, const TExpression* root);

//! Recursively applies the functor to the tree.
template <class TNode, class TFunctor>
const TNode* Apply(TQueryContext* context, const TNode* node, const TFunctor& functor)
{
    const TNode* result = functor(context, node);

    auto immutableChildren = result->Children();
    auto mutableChildren = TMutableArrayRef<const TNode*>(
        const_cast<const TNode**>(immutableChildren.data()),
        immutableChildren.size());

    for (auto& child : mutableChildren) {
        child = Apply(context, child, functor);
    }

    return result;
}

//! Recursively visits every node in the tree.
template <class TNode, class TVisitor>
void Visit(const TNode* node, const TVisitor& visitor)
{
    visitor(node);
    for (const auto& child : node->Children()) {
        Visit(child, visitor);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

