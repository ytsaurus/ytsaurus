#pragma once

#include <core/misc/array_ref.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TOperator;
class TExpression;

class TPlanContext;
struct IPlanVisitor;

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
const TOperator* Apply(
    TPlanContext* context,
    const TOperator* root,
    const std::function<const TOperator*(TPlanContext*, const TOperator*)>& functor);
const TExpression* Apply(
    TPlanContext* context,
    const TExpression* root,
    const std::function<const TExpression*(TPlanContext*, const TExpression*)>& functor);

//! Recursively visits every node in the tree.
void Visit(
    const TExpression* root,
    const std::function<void(const TExpression*)>& visitor);
void Visit(
    const TOperator* root,
    const std::function<void(const TOperator*)>& visitor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

