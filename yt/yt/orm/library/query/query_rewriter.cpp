#include "query_rewriter.h"

#include "misc.h"

#include <yt/yt/library/query/base/expr_builder_base.h>

#include <limits>
#include <utility>

namespace NYT::NOrm::NQuery {

using namespace NQueryClient::NAst;

////////////////////////////////////////////////////////////////////////////////

namespace {

enum class EComparisonResult
{
    Unsupported,
    False,
    True,
};

std::optional<TLiteralValue> TryExtractComparisonLiteral(const TExpressionList& expressions)
{
    if (auto value = TryExtractSingleLiteralValue(expressions)) {
        return value;
    }
    if (std::ssize(expressions) == 1) {
        auto* unary = expressions[0]->As<TUnaryOpExpression>();
        if (unary && unary->Opcode == NQueryClient::EUnaryOp::Minus) {
            if (auto value = TryExtractSingleLiteralValue(unary->Operand)) {
                return std::visit([] (const auto& literal) -> std::optional<TLiteralValue> {
                    using T = std::decay_t<decltype(literal)>;
                    if constexpr (std::is_same_v<T, i64>) {
                        if (literal != std::numeric_limits<i64>::min()) {
                            return -literal;
                        }
                    } else if constexpr (std::is_same_v<T, double>) {
                        return -literal;
                    }
                    return std::nullopt;
                }, *value);
            }
        }
    }
    return std::nullopt;
}

EComparisonResult CompareLiterals(
    const TLiteralValue& lhs,
    const TLiteralValue& rhs,
    NQueryClient::EBinaryOp opcode)
{
    auto result = NQueryClient::FoldConstants(
        opcode,
        New<NQueryClient::TLiteralExpression>(
            NQueryClient::GetType(lhs),
            NQueryClient::TOwningValue(NQueryClient::GetValue(lhs))),
        New<NQueryClient::TLiteralExpression>(
            NQueryClient::GetType(rhs),
            NQueryClient::TOwningValue(NQueryClient::GetValue(rhs))));
    if (!result || result->Type != NTableClient::EValueType::Boolean) {
        return EComparisonResult::Unsupported;
    }
    return result->Data.Boolean ? EComparisonResult::True : EComparisonResult::False;
}

TExpressionPtr BuildNullAwarePredicate(
    TObjectsHolder* holder,
    TExpressionPtr expression,
    TExpressionPtr reference,
    bool matchesDefault)
{
    auto* isNull = holder->New<TBinaryOpExpression>(
        NQueryClient::TSourceLocation(),
        NQueryClient::EBinaryOp::Equal,
        TExpressionList{reference},
        TExpressionList{holder->New<TLiteralExpression>(NQueryClient::TSourceLocation(), TNullLiteralValue{})});
    if (matchesDefault) {
        return BuildOrExpression(holder, isNull, expression);
    }
    auto* notNull = holder->New<TUnaryOpExpression>(
        NQueryClient::TSourceLocation(),
        NQueryClient::EUnaryOp::Not,
        TExpressionList{isNull});
    return BuildAndExpression(holder, notNull, expression);
}

TExpressionPtr TryRewriteNullAsDefaultComparison(
    TObjectsHolder* holder,
    TBinaryOpExpression* binary,
    const TReferenceDefaultValueGetter& getDefaultValue)
{
    if (!NQueryClient::IsRelationalBinaryOp(binary->Opcode)) {
        return nullptr;
    }
    auto lhsLiteral = TryExtractComparisonLiteral(binary->Lhs);
    auto rhsLiteral = TryExtractComparisonLiteral(binary->Rhs);
    auto reference = TryExtractReference(binary->Lhs);
    auto literal = rhsLiteral;
    auto opcode = binary->Opcode;
    const auto* referenceSide = &binary->Lhs;
    if (!reference || !literal) {
        reference = TryExtractReference(binary->Rhs);
        literal = lhsLiteral;
        opcode = NQueryClient::GetReversedBinaryOpcode(opcode);
        referenceSide = &binary->Rhs;
    }
    if (!reference || !literal || std::holds_alternative<TNullLiteralValue>(*literal)) {
        return nullptr;
    }
    auto* defaultExpression = getDefaultValue(*reference);
    const auto* defaultValue = defaultExpression ? defaultExpression->As<TLiteralExpression>() : nullptr;
    if (!defaultValue) {
        return nullptr;
    }
    auto comparison = CompareLiterals(defaultValue->Value, *literal, opcode);
    if (comparison == EComparisonResult::Unsupported) {
        return nullptr;
    }
    if (comparison == CompareLiterals(TNullLiteralValue{}, *literal, opcode)) {
        return binary;
    }

    auto* referenceExpression = (*referenceSide)[0];
    if (opcode == NQueryClient::EBinaryOp::Equal) {
        return holder->New<TInExpression>(
            NQueryClient::TSourceLocation(),
            TExpressionList{referenceExpression},
            TLiteralValueTupleList{{TNullLiteralValue{}}, {*literal}});
    }
    return BuildNullAwarePredicate(holder, binary, referenceExpression, comparison == EComparisonResult::True);
}

TExpressionPtr TryRewriteNullAsDefaultIn(
    TObjectsHolder* holder,
    TInExpression* in,
    const TReferenceDefaultValueGetter& getDefaultValue)
{
    auto reference = TryExtractReference(in->Expr);
    if (!reference) {
        return nullptr;
    }
    auto* defaultExpression = getDefaultValue(*reference);
    const auto* defaultValue = defaultExpression ? defaultExpression->As<TLiteralExpression>() : nullptr;
    if (!defaultValue) {
        return nullptr;
    }

    bool matchesDefault = false;
    bool containsNull = false;
    for (const auto& tuple : in->Values) {
        if (std::ssize(tuple) != 1) {
            return nullptr;
        }
        if (std::holds_alternative<TNullLiteralValue>(tuple[0])) {
            containsNull = true;
            continue;
        }
        auto comparison = CompareLiterals(defaultValue->Value, tuple[0], NQueryClient::EBinaryOp::Equal);
        if (comparison == EComparisonResult::Unsupported) {
            return nullptr;
        }
        matchesDefault |= comparison == EComparisonResult::True;
    }
    if (matchesDefault == containsNull) {
        return in;
    }
    auto values = in->Values;
    if (matchesDefault) {
        values.insert(values.begin(), TLiteralValueTuple{TNullLiteralValue{}});
    } else {
        std::erase_if(values, [] (const auto& tuple) {
            return std::holds_alternative<TNullLiteralValue>(tuple[0]);
        });
        if (values.empty()) {
            return holder->New<TLiteralExpression>(NQueryClient::TSourceLocation(), false);
        }
    }
    return holder->New<TInExpression>(NQueryClient::TSourceLocation(), in->Expr, std::move(values));
}

TExpressionPtr TryWrapReferenceWithIfNull(
    TObjectsHolder* holder,
    TExpressionPtr expression,
    const TReferenceDefaultValueGetter& getDefaultValue)
{
    auto* reference = expression->As<TReferenceExpression>();
    if (!reference) {
        return nullptr;
    }
    auto* defaultValue = getDefaultValue(reference->Reference);
    if (!defaultValue) {
        return nullptr;
    }
    return holder->New<TFunctionExpression>(
        NQueryClient::TSourceLocation(),
        "if_null",
        TExpressionList{
            expression,
            defaultValue,
        });
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TExpressionPtr RewriteNullAsDefaultPredicate(
    TObjectsHolder* holder,
    TExpressionPtr expression,
    const TReferenceDefaultValueGetter& getDefaultValue)
{
    if (auto* function = expression->As<TFunctionExpression>(); function && function->FunctionName == "is_null") {
        if (auto reference = TryExtractReference(function->Arguments);
            reference && getDefaultValue(*reference))
        {
            return holder->New<TLiteralExpression>(NQueryClient::TSourceLocation(), false);
        }
    }
    if (auto* binary = expression->As<TBinaryOpExpression>()) {
        if (auto* rewritten = TryRewriteNullAsDefaultComparison(holder, binary, getDefaultValue)) {
            return rewritten;
        }
    }
    if (auto* in = expression->As<TInExpression>()) {
        if (auto* rewritten = TryRewriteNullAsDefaultIn(holder, in, getDefaultValue)) {
            return rewritten;
        }
    }
    return TryWrapReferenceWithIfNull(holder, expression, getDefaultValue);
}

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
        [] (const auto& /*value*/) -> TLiteralValue {
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
