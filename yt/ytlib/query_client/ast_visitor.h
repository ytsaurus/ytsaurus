#pragma once

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TOperator;
class TExpression;

class TScanOperator;
class TFilterOperator;
class TProjectOperator;

#define AS_INTERFACE(nodeType) \
    virtual bool Visit(T ## nodeType *) = 0;
#define AS_IMPLEMENTATION(nodeType) \
    virtual bool Visit(T ## nodeType *) override;

struct IAstVisitor
{
    virtual ~IAstVisitor()
    { }

#define XX(nodeType) AS_INTERFACE(nodeType)
#include "list_of_expressions.inc"
#include "list_of_operators.inc"
#undef XX
};

struct IExpressionAstVisitor
    : IAstVisitor
{
#define XX(nodeType) AS_IMPLEMENTATION(nodeType)
#include "list_of_operators.inc"
#undef XX
};

struct IOperatorAstVisitor
    : IAstVisitor
{
#define XX(nodeType) AS_IMPLEMENTATION(nodeType)
#include "list_of_expressions.inc"
#undef XX
};

class TAstVisitor
    : public IAstVisitor
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
bool visitorType::Visit(T ## nodeType*) \
{ return true; }
#define IMPLEMENT_AST_VISITOR_HOOK(nodeType) \
bool T ## nodeType::Accept(IAstVisitor* visitor) \
{ return visitor->Visit(this); }

//! Runs a depth-first preorder traversal.
bool Traverse(IAstVisitor* visitor, TOperator* root);
bool Traverse(IAstVisitor* visitor, TExpression* root);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

