#include "query_rewriter.h"

namespace NYT::NOrm::NQuery {

using namespace NQueryClient::NAst;

////////////////////////////////////////////////////////////////////////////////

TExpressionPtr DummyFunctionRewriter(TFunctionExpression*)
{
    return nullptr;
}

TExpressionPtr DummyReferenceMapping(const TReference&)
{
    return nullptr;
}

TExpressionPtr DummyExpressionRewriter(TExpressionPtr)
{
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

TQueryRewriter::TQueryRewriter(
    TObjectsHolder* holder,
    TReferenceMapping referenceMapping,
    TFunctionRewriter functionRewriter,
    TExpressionRewriter expressionRewriter)
    : TRewriter(holder)
    , ReferenceMapping_(std::move(referenceMapping))
    , FunctionRewriter_(std::move(functionRewriter))
    , ExpressionRewriter_(std::move(expressionRewriter))
{
    YT_VERIFY(ReferenceMapping_);
    YT_VERIFY(FunctionRewriter_);
    YT_VERIFY(ExpressionRewriter_);
}

TExpressionPtr TQueryRewriter::Run(const TExpressionPtr& expr)
{
    TExpressionPtr expr_(expr);
    return Visit(expr_);
}

TExpressionPtr TQueryRewriter::OnReference(TReferenceExpressionPtr referenceExpr)
{
    if (auto* newExpr = ExpressionRewriter_(referenceExpr)) {
        return newExpr;
    }
    if (auto* newExpr = ReferenceMapping_(referenceExpr->Reference)) {
        return newExpr;
    }
    return TRewriter::OnReference(referenceExpr);
}

TExpressionPtr TQueryRewriter::OnFunction(TFunctionExpressionPtr functionExpr)
{
    if (auto* newExpr = ExpressionRewriter_(functionExpr)) {
        return newExpr;
    }
    if (auto* newExpr = FunctionRewriter_(functionExpr)) {
        return newExpr;
    }
    return TRewriter::OnFunction(functionExpr);
}

TExpressionPtr TQueryRewriter::OnLiteral(TLiteralExpressionPtr literalExpr)
{
    if (auto* newExpr = ExpressionRewriter_(literalExpr)) {
        return newExpr;
    }
    return TRewriter::OnLiteral(literalExpr);
}

TExpressionPtr TQueryRewriter::OnAlias(TAliasExpressionPtr aliasExpr)
{
    if (auto* newExpr = ExpressionRewriter_(aliasExpr)) {
        return newExpr;
    }
    return TRewriter::OnAlias(aliasExpr);
}

TExpressionPtr TQueryRewriter::OnUnary(TUnaryOpExpressionPtr unaryExpr)
{
    if (auto* newExpr = ExpressionRewriter_(unaryExpr)) {
        return newExpr;
    }
    return TRewriter::OnUnary(unaryExpr);
}

TExpressionPtr TQueryRewriter::OnBinary(TBinaryOpExpressionPtr binaryExpr)
{
    if (auto* newExpr = ExpressionRewriter_(binaryExpr)) {
        return newExpr;
    }
    return TRewriter::OnBinary(binaryExpr);
}

TExpressionPtr TQueryRewriter::OnIn(TInExpressionPtr inExpr)
{
    if (auto* newExpr = ExpressionRewriter_(inExpr)) {
        return newExpr;
    }
    return TRewriter::OnIn(inExpr);
}

TExpressionPtr TQueryRewriter::OnBetween(TBetweenExpressionPtr betweenExpr)
{
    if (auto* newExpr = ExpressionRewriter_(betweenExpr)) {
        return newExpr;
    }
    return TRewriter::OnBetween(betweenExpr);
}

TExpressionPtr TQueryRewriter::OnTransform(TTransformExpressionPtr transformExpr)
{
    if (auto* newExpr = ExpressionRewriter_(transformExpr)) {
        return newExpr;
    }
    return TRewriter::OnTransform(transformExpr);
}

TExpressionPtr TQueryRewriter::OnCase(TCaseExpressionPtr caseExpr)
{
    if (auto* newExpr = ExpressionRewriter_(caseExpr)) {
        return newExpr;
    }
    return TRewriter::OnCase(caseExpr);
}

TExpressionPtr TQueryRewriter::OnLike(TLikeExpressionPtr likeExpr)
{
    if (auto* newExpr = ExpressionRewriter_(likeExpr)) {
        return newExpr;
    }
    return TRewriter::OnLike(likeExpr);
}

TExpressionPtr TQueryRewriter::OnQuery(TQueryExpressionPtr queryExpr)
{
    if (auto* newExpr = ExpressionRewriter_(queryExpr)) {
        return newExpr;
    }
    return TRewriter::OnQuery(queryExpr);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TLiteralValue TryBitInvertLiteral(TLiteralValue value)
{
    return Visit(value,
        [] (ui64 value) -> TLiteralValue {
            return ~value;
        },
        [] (i64 value) -> TLiteralValue {
            if (value < 0) {
                return TNullLiteralValue{};
            } else {
                return ~static_cast<ui64>(value);
            }
        },
        [] (const auto& _) -> TLiteralValue {
            return TNullLiteralValue{};
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TBitNotQueryRewriter::TBitNotQueryRewriter(
    TObjectsHolder* holder,
    std::string referenceName,
    TReference targetReference,
    bool invertExpressions,
    std::function<void(const NQueryClient::NAst::TReference&)> onUnexpectedReference)
    : TRewriter(holder)
    , ExpectedReference_(std::move(referenceName))
    , TargetReference_(std::move(targetReference))
    , InvertExpressions_(invertExpressions)
    , OnUnexpectedReference_(std::move(onUnexpectedReference))
{ }

TExpressionPtr TBitNotQueryRewriter::OnBinary(TBinaryOpExpressionPtr binaryExpr)
{
    if (InvertExpressions_ &&
        NQueryClient::IsRelationalBinaryOp(binaryExpr->Opcode) &&
        std::ssize(binaryExpr->Lhs) == 1 &&
        std::ssize(binaryExpr->Rhs) == 1)
    {
        TReferenceExpressionPtr referenceExpr;
        TLiteralExpressionPtr literalExpr;
        auto opCode = binaryExpr->Opcode;
        if (binaryExpr->Lhs[0]->As<TReferenceExpression>()) {
            referenceExpr = binaryExpr->Lhs[0]->As<TReferenceExpression>();
            literalExpr = binaryExpr->Rhs[0]->As<TLiteralExpression>();
            opCode = NQueryClient::GetReversedBinaryOpcode(opCode);
        } else {
            referenceExpr = binaryExpr->Rhs[0]->As<TReferenceExpression>();
            literalExpr = binaryExpr->Lhs[0]->As<TLiteralExpression>();
        }
        TLiteralValue literalValue = literalExpr
            ? TryBitInvertLiteral(literalExpr->Value)
            : TNullLiteralValue{};

        if (referenceExpr &&
            referenceExpr->Reference == ExpectedReference_ &&
            !std::get_if<TNullLiteralValue>(&literalValue))
        {
            return Head->New<TBinaryOpExpression>(
                NQueryClient::TSourceLocation(),
                opCode,
                TExpressionList{Head->New<TReferenceExpression>(NQueryClient::TSourceLocation(), TargetReference_)},
                TExpressionList{Head->New<TLiteralExpression>(NQueryClient::TSourceLocation(), literalValue)});
        }
    }

    binaryExpr->Lhs = Visit(binaryExpr->Lhs);
    binaryExpr->Rhs = Visit(binaryExpr->Rhs);

    return binaryExpr;
}

TExpressionPtr TBitNotQueryRewriter::OnReference(
    TReferenceExpressionPtr referenceExpr)
{
    if (referenceExpr->Reference == ExpectedReference_) {
        referenceExpr->Reference = TargetReference_;
        return InvertExpressions_
            ? Head->New<TUnaryOpExpression>(
                NQueryClient::TSourceLocation(),
                NQueryClient::EUnaryOp::BitNot,
                TExpressionList{referenceExpr})
            : static_cast<TExpressionPtr>(referenceExpr);
    }

    if (OnUnexpectedReference_) {
        OnUnexpectedReference_(referenceExpr->Reference);
    }
    return referenceExpr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
