#pragma once

#include <core/misc/array_ref.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TOperator;
class TExpression;

class TPlanContext;
struct IPlanVisitor;

#define DECLARE_VISITOR_INTERFACE(type, kind) \
    virtual bool Visit(const T##kind##type *) = 0;
#define DECLARE_VISITOR_IMPLEMENTATION(type, kind) \
    virtual bool Visit(const T##kind##type *) override;

struct IPlanVisitor
{
    virtual ~IPlanVisitor()
    { }
#define XX(kind) DECLARE_VISITOR_INTERFACE(Operator, kind)
#include "list_of_operators.inc"
#undef XX
#define XX(kind) DECLARE_VISITOR_INTERFACE(Expression, kind)
#include "list_of_expressions.inc"
#undef XX
};

class TPlanVisitor
    : public IPlanVisitor
{
#define XX(kind) DECLARE_VISITOR_IMPLEMENTATION(Operator, kind)
#include "list_of_operators.inc"
#undef XX
#define XX(kind) DECLARE_VISITOR_IMPLEMENTATION(Expression, kind)
#include "list_of_expressions.inc"
#undef XX
};

#undef DECLARE_VISITOR_INTERFACE
#undef DECLARE_VISITOR_IMPLEMENTATION

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

