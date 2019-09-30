#include "query_rewriter.h"

namespace NYP::NServer::NQueryHelpers {

using namespace NYT::NQueryClient::NAst;

////////////////////////////////////////////////////////////////////////////////

TQueryRewriter::TQueryRewriter(TReferenceMapping referenceMapping)
    : ReferenceMapping_(std::move(referenceMapping))
{
    YT_VERIFY(ReferenceMapping_);
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
        Visit(typedExpr->Arguments);
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

} // namespace NYP::NServer::NQueryHelpers
