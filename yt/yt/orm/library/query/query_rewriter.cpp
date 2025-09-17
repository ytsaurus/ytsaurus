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

////////////////////////////////////////////////////////////////////////////////

TQueryRewriter::TQueryRewriter(
    TObjectsHolder* holder,
    TReferenceMapping referenceMapping,
    TFunctionRewriter functionRewriter)
    : TRewriter(holder)
    , ReferenceMapping_(std::move(referenceMapping))
    , FunctionRewriter_(std::move(functionRewriter))
{
    YT_VERIFY(ReferenceMapping_);
    YT_VERIFY(FunctionRewriter_);
}

TExpressionPtr TQueryRewriter::Run(const TExpressionPtr& expr)
{
    TExpressionPtr expr_(expr);
    return Visit(expr_);
}

TExpressionPtr TQueryRewriter::OnReference(TReferenceExpressionPtr referenceExpr)
{
    if (auto* newExpr = ReferenceMapping_(referenceExpr->Reference)) {
        return newExpr;
    }
    return referenceExpr;
}

TExpressionPtr TQueryRewriter::OnFunction(TFunctionExpressionPtr functionExpr)
{
    if (auto* newExpr = FunctionRewriter_(functionExpr)) {
        return newExpr;
    }
    return TRewriter::OnFunction(functionExpr);
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
        }
    );
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
