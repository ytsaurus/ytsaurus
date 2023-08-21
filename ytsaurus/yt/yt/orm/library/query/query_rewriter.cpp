#include "query_rewriter.h"

namespace NYT::NOrm::NQuery {

using namespace NQueryClient::NAst;

////////////////////////////////////////////////////////////////////////////////

TExpressionPtr DummyFunctionRewriter(TFunctionExpression*)
{
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

TQueryRewriter::TQueryRewriter(
    TReferenceMapping referenceMapping,
    TFunctionRewriter functionRewriter)
    : ReferenceMapping_(std::move(referenceMapping))
    , FunctionRewriter_(std::move(functionRewriter))
{
    YT_VERIFY(ReferenceMapping_);
    YT_VERIFY(FunctionRewriter_);
}

TExpressionPtr TQueryRewriter::Run(const TExpressionPtr& expr)
{
    TExpressionPtr expr_(expr);
    Visit(&expr_);
    return expr_;
}

void TQueryRewriter::Visit(TExpressionPtr* expr)
{
    if ((*expr)->As<TLiteralExpression>()) {
        // Do nothing.
    } else if (auto* typedExpr = (*expr)->As<TReferenceExpression>()) {
        *expr = ReferenceMapping_(typedExpr->Reference);
    } else if (auto* typedExpr = (*expr)->As<TAliasExpression>()) {
        Visit(&typedExpr->Expression);
    } else if (auto* typedExpr = (*expr)->As<TFunctionExpression>()) {
        if (auto newExpr = FunctionRewriter_(typedExpr)) {
            *expr = newExpr;
        } else {
            Visit(typedExpr->Arguments);
        }
    } else if (auto* typedExpr = (*expr)->As<TUnaryOpExpression>()) {
        Visit(typedExpr->Operand);
    } else if (auto* typedExpr = (*expr)->As<TBinaryOpExpression>()) {
        Visit(typedExpr->Lhs);
        Visit(typedExpr->Rhs);
    } else if (auto* typedExpr = (*expr)->As<TInExpression>()) {
        Visit(typedExpr->Expr);
    } else if (auto* typedExpr = (*expr)->As<TBetweenExpression>()) {
        Visit(typedExpr->Expr);
    } else if (auto* typedExpr = (*expr)->As<TTransformExpression>()) {
        Visit(typedExpr->Expr);
        Visit(typedExpr->DefaultExpr);
    } else {
        YT_ABORT();
    }
}

void TQueryRewriter::Visit(TNullableExpressionList& list)
{
    if (list) {
        Visit(*list);
    }
}

void TQueryRewriter::Visit(TExpressionList& list)
{
    for (auto& expr : list) {
        Visit(&expr);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
