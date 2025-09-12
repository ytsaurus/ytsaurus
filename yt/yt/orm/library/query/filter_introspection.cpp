#include "filter_introspection.h"

#include "misc.h"

#include <yt/yt/orm/library/attributes/attribute_path.h>

#include <yt/yt/library/query/base/ast_visitors.h>
#include <yt/yt/library/query/base/query_preparer.h>

namespace NYT::NOrm::NQuery {

using namespace NQueryClient::NAst;
using namespace NYPath;

using NQueryClient::EBinaryOp;

////////////////////////////////////////////////////////////////////////////////

namespace {

TOptionalLiteralValueWrapper TryCastToLiteralValue(const TExpressionList& exprList) noexcept
{
    if (exprList.size() == 1) {
        if (auto* typedExpr = exprList[0]->As<TLiteralExpression>()) {
            return typedExpr->Value;
        }
    }
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

class TQueryVisitorForDefinedAttributeValue
    : public TBaseAstVisitor<TOptionalLiteralValueWrapper, TQueryVisitorForDefinedAttributeValue>
{
public:
    explicit TQueryVisitorForDefinedAttributeValue(const TYPath& attributePath)
        : AttributePath_(attributePath)
    { }

    TOptionalLiteralValueWrapper Run(TExpressionPtr expression)
    {
        return Visit(expression);
    }

    TOptionalLiteralValueWrapper OnLiteral(TLiteralExpressionPtr /*literalExpr*/)
    {
        return std::nullopt;
    }

    TOptionalLiteralValueWrapper OnReference(TReferenceExpressionPtr /*referenceExpr*/)
    {
        return std::nullopt;
    }

    TOptionalLiteralValueWrapper OnAlias(TAliasExpressionPtr /*aliasExpr*/)
    {
        return std::nullopt;
    }

    TOptionalLiteralValueWrapper OnUnary(TUnaryOpExpressionPtr /*unaryExpr*/)
    {
        return std::nullopt;
    }

    TOptionalLiteralValueWrapper OnBinary(TBinaryOpExpressionPtr binaryExpr)
    {
        switch (binaryExpr->Opcode) {
            case EBinaryOp::Equal: {
                if (IsAttributeReference(binaryExpr->Lhs, AttributePath_)) {
                    if (auto value = TryCastToLiteralValue(binaryExpr->Rhs)) {
                        return value;
                    }
                } else if (IsAttributeReference(binaryExpr->Rhs, AttributePath_)) {
                    if (auto value = TryCastToLiteralValue(binaryExpr->Lhs)) {
                        return value;
                    }
                }
                return std::nullopt;
            }
            case EBinaryOp::Or: {
                auto lhs = Visit(binaryExpr->Lhs);
                auto rhs = Visit(binaryExpr->Rhs);
                if (lhs == rhs) {
                    return lhs;
                } else {
                    return std::nullopt;
                }
            }
            case EBinaryOp::And: {
                auto lhs = Visit(binaryExpr->Lhs);
                auto rhs = Visit(binaryExpr->Rhs);
                if (lhs) {
                    return lhs;
                }
                if (rhs) {
                    return rhs;
                }
                return std::nullopt;
            }
            default:
                return std::nullopt;
        }
    }

    TOptionalLiteralValueWrapper OnFunction(TFunctionExpressionPtr /*functionExpr*/)
    {
        return std::nullopt;
    }

    TOptionalLiteralValueWrapper OnIn(TInExpressionPtr /*inExpr*/)
    {
        return std::nullopt;
    }

    TOptionalLiteralValueWrapper OnBetween(TBetweenExpressionPtr /*betweenExpr*/)
    {
        return std::nullopt;
    }

    TOptionalLiteralValueWrapper OnTransform(TTransformExpressionPtr /*transformExpr*/)
    {
        return std::nullopt;
    }

    TOptionalLiteralValueWrapper OnCase(TCaseExpressionPtr /*caseExpr*/)
    {
        return std::nullopt;
    }

    TOptionalLiteralValueWrapper OnLike(TLikeExpressionPtr /*likeExpr*/)
    {
        return std::nullopt;
    }

private:
    const TYPath& AttributePath_;

    using TBaseAstVisitor<TOptionalLiteralValueWrapper, TQueryVisitorForDefinedAttributeValue>::Visit;

    TOptionalLiteralValueWrapper Visit(const TExpressionList& exprList)
    {
        if (exprList.size() == 1) {
            return Visit(exprList[0]);
        }
        return std::nullopt;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TQueryVisitorForDefinedReference
    : public TBaseAstVisitor<bool, TQueryVisitorForDefinedReference>
{
public:
    TQueryVisitorForDefinedReference(
        const NQueryClient::NAst::TReference& reference,
        bool allowValueRange)
        : Reference_(reference)
        , AllowValueRange_(allowValueRange)
    { }

    bool Run(TExpressionPtr expression)
    {
        return Visit(expression);
    }

    bool OnLiteral(TLiteralExpressionPtr /*literalExpr*/)
    {
        return false;
    }

    bool OnReference(TReferenceExpressionPtr /*referenceExpr*/)
    {
        return false;
    }

    bool OnAlias(TAliasExpressionPtr /*aliasExpr*/)
    {
        return false;
    }

    bool OnUnary(TUnaryOpExpressionPtr /*unaryExpr*/)
    {
        return false;
    }

    bool OnBinary(TBinaryOpExpressionPtr binaryExpr)
    {
        switch (binaryExpr->Opcode) {
            case EBinaryOp::Or:
                return Visit(binaryExpr->Lhs) && Visit(binaryExpr->Rhs);
            case EBinaryOp::And:
                return Visit(binaryExpr->Lhs) || Visit(binaryExpr->Rhs);
            case EBinaryOp::NotEqual:
            case EBinaryOp::Less:
            case EBinaryOp::LessOrEqual:
            case EBinaryOp::Greater:
            case EBinaryOp::GreaterOrEqual:
                if (!AllowValueRange_) {
                    return false;
                }
                [[fallthrough]];
            case EBinaryOp::Equal:
                return IsTargetReference(binaryExpr->Lhs, Reference_) || IsTargetReference(binaryExpr->Rhs, Reference_);
            default:
                return false;
        }
    }

    bool OnFunction(TFunctionExpressionPtr /*functionExpr*/)
    {
        return false;
    }

    bool OnIn(TInExpressionPtr inExpr)
    {
        return IsAnyExprATargetReference(inExpr->Expr, Reference_);
    }

    bool OnBetween(TBetweenExpressionPtr betweenExpr)
    {
        return IsAnyExprATargetReference(betweenExpr->Expr, Reference_) && AllowValueRange_;
    }

    bool OnTransform(TTransformExpressionPtr /*transformExpr*/)
    {
        return false;
    }

    bool OnCase(TCaseExpressionPtr /*caseExpr*/)
    {
        return false;
    }

    bool OnLike(TLikeExpressionPtr /*likeExpr*/)
    {
        return false;
    }

private:
    const NQueryClient::NAst::TReference Reference_;
    const bool AllowValueRange_;

    using TBaseAstVisitor<bool, TQueryVisitorForDefinedReference>::Visit;

    bool Visit(const TExpressionList& exprs)
    {
        if (exprs.size() != 1) {
            return false;
        }
        return Visit(exprs[0]);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TQueryVisitorForDefinedAlwaysFalse
    : public TBaseAstVisitor<bool, TQueryVisitorForDefinedAlwaysFalse>
{
public:
    TQueryVisitorForDefinedAlwaysFalse()
    { }

    bool Run(const TExpressionPtr expression)
    {
        return Visit(expression);
    }

    bool OnLiteral(const TLiteralExpressionPtr literalExpr)
    {
        auto* literalValue = std::get_if<bool>(&literalExpr->Value);
        return literalValue && !*literalValue;
    }

    bool OnReference(const TReferenceExpressionPtr /*referenceExpr*/)
    {
        return false;
    }

    bool OnAlias(const TAliasExpressionPtr /*aliasExpr*/)
    {
        return false;
    }

    bool OnUnary(const TUnaryOpExpressionPtr /*unaryExpr*/)
    {
        return false;
    }

    bool OnBinary(const TBinaryOpExpressionPtr binaryExpr)
    {
        switch (binaryExpr->Opcode) {
            case EBinaryOp::Or:
                return Visit(binaryExpr->Lhs) && Visit(binaryExpr->Rhs);
            case EBinaryOp::And:
                return Visit(binaryExpr->Lhs) || Visit(binaryExpr->Rhs);
            default:
                return false;
        }
    }

    bool OnFunction(const TFunctionExpressionPtr /*functionExpr*/)
    {
        return false;
    }

    bool OnIn(const TInExpressionPtr /*inExpr*/)
    {
        return false;
    }

    bool OnBetween(const TBetweenExpressionPtr /*betweenExpr*/)
    {
        return false;
    }

    bool OnTransform(const TTransformExpressionPtr /*transformExpr*/)
    {
        return false;
    }

    bool OnCase(const TCaseExpressionPtr /*caseExpr*/)
    {
        return false;
    }

    bool OnLike(const TLikeExpressionPtr /*likeExpr*/)
    {
        return false;
    }

private:
    using TBaseAstVisitor<bool, TQueryVisitorForDefinedAlwaysFalse>::Visit;

    bool Visit(const TExpressionList& expressions)
    {
        if (expressions.size() == 1) {
            return Visit(expressions[0]);
        }
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TQueryVisitorForAttributeReferences
    : public TAstVisitor<TQueryVisitorForAttributeReferences>
{
public:
    explicit TQueryVisitorForAttributeReferences(std::function<void(const std::string&)> inserter)
        : Inserter_(std::move(inserter))
    { }

    void Run(TExpressionPtr expression)
    {
        Visit(expression);
    }

    void OnReference(TReferenceExpressionPtr referenceExpr)
    {
        THROW_ERROR_EXCEPTION_IF(
            referenceExpr->Reference.TableName,
            "Invalid attribute reference %Qv",
            FormatExpression(*referenceExpr));
        Inserter_(referenceExpr->Reference.ColumnName);
    }

    void OnAlias(TAliasExpressionPtr aliasExpr)
    {
        Visit(aliasExpr->Expression);
    }

    void OnTransform(TTransformExpressionPtr transformExpr)
    {
        Visit(transformExpr->Expr);
        if (auto defaultExpr = transformExpr->DefaultExpr) {
            Visit(*defaultExpr);
        }
    }

private:
    const std::function<void(const std::string)> Inserter_;

    using TAstVisitor<TQueryVisitorForAttributeReferences>::Visit;
};

////////////////////////////////////////////////////////////////////////////////

class TQueryVisitorForInValueCount
    : public TBaseAstVisitor<int, TQueryVisitorForInValueCount>
{
public:
    TQueryVisitorForInValueCount()
    { }

    int Run(const TExpressionPtr expression)
    {
        return Visit(expression);
    }

    int OnLiteral(const TLiteralExpressionPtr /*literalExpr*/)
    {
        return 0;
    }

    int OnReference(const TReferenceExpressionPtr /*referenceExpr*/)
    {
        return 0;
    }

    int OnAlias(const TAliasExpressionPtr /*aliasExpr*/)
    {
        return 0;
    }

    int OnUnary(const TUnaryOpExpressionPtr unaryExpr)
    {
        return Visit(unaryExpr->Operand);
    }

    int OnBinary(const TBinaryOpExpressionPtr binaryExpr)
    {
        return Visit(binaryExpr->Lhs) + Visit(binaryExpr->Rhs);
    }

    int OnFunction(const TFunctionExpressionPtr functionExpr)
    {
        return Visit(functionExpr->Arguments);
    }

    int OnIn(const TInExpressionPtr inExpr)
    {
        return Visit(inExpr->Expr) + inExpr->Values.size();
    }

    // Sometimes BETWEEN clauses are rewritten as `a <= x AND x <= b` before visiting, and this can double the count result.
    int OnBetween(const TBetweenExpressionPtr betweenExpr)
    {
        return Visit(betweenExpr->Expr);
    }

    int OnTransform(const TTransformExpressionPtr transformExpr)
    {
        return Visit(transformExpr->Expr) + Visit(transformExpr->DefaultExpr);
    }

    int OnCase(const TCaseExpressionPtr caseExpr)
    {
        int res = Visit(caseExpr->OptionalOperand) + Visit(caseExpr->DefaultExpression);
        for (const auto& whenThenExpression : caseExpr->WhenThenExpressions) {
            res += Visit(whenThenExpression.Condition);
            res += Visit(whenThenExpression.Result);
        }
        return res;
    }

    int OnLike(const TLikeExpressionPtr likeExpr)
    {
        return Visit(likeExpr->Text) + Visit(likeExpr->Pattern) + Visit(likeExpr->EscapeCharacter);
    }

private:
    using TBaseAstVisitor<int, TQueryVisitorForInValueCount>::Visit;

    int Visit(const TExpressionList& expressions)
    {
        int result = 0;
        for (auto expression : expressions) {
            result += Visit(expression);
        }
        return result;
    }

    int Visit(const TNullableExpressionList& nullableTuple)
    {
        if (nullableTuple) {
            return Visit(*nullableTuple);
        }
        return 0;
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TOptionalLiteralValueWrapper IntrospectFilterForDefinedAttributeValue(
    const std::string& filterQuery,
    const TYPath& attributePath)
{
    NAttributes::ValidateAttributePath(attributePath);
    if (!attributePath) {
        THROW_ERROR_EXCEPTION("Attribute path must be non-empty");
    }
    if (filterQuery.empty()) {
        return std::nullopt;
    }
    auto parsedQuery = ParseSource(filterQuery, NQueryClient::EParseMode::Expression);
    auto* queryExpression = std::get<TExpressionPtr>(parsedQuery->AstHead.Ast);
    return TQueryVisitorForDefinedAttributeValue(attributePath).Run(queryExpression);
}

////////////////////////////////////////////////////////////////////////////////

bool IntrospectFilterForDefinedReference(
    TExpressionPtr filterExpression,
    const NQueryClient::NAst::TReference& reference,
    bool allowValueRange)
{
    return TQueryVisitorForDefinedReference(reference, allowValueRange)
        .Run(filterExpression);
}

////////////////////////////////////////////////////////////////////////////////

int IntrospectFilterInValueCount(TExpressionPtr filterExpression)
{
    return TQueryVisitorForInValueCount().Run(filterExpression);
}

////////////////////////////////////////////////////////////////////////////////

void ExtractFilterAttributeReferences(TStringBuf filterQuery, std::function<void(TYPathBuf)> inserter)
{
    if (filterQuery.empty()) {
        return;
    }
    auto parsedQuery = ParseSource(filterQuery, NQueryClient::EParseMode::Expression);
    auto* queryExpression = std::get<TExpressionPtr>(parsedQuery->AstHead.Ast);

    TQueryVisitorForAttributeReferences(std::move(inserter)).Run(queryExpression);
}

void ExtractFilterAttributeReferences(TExpressionPtr filterExpression, std::function<void(TYPathBuf)> inserter)
{
    TQueryVisitorForAttributeReferences(std::move(inserter)).Run(filterExpression);
}

////////////////////////////////////////////////////////////////////////////////

bool IsAttributeReference(const TExpressionList& exprList, TYPathBuf attributePath) noexcept
{
    if (exprList.size() == 1) {
        if (auto* typedExpr = exprList[0]->As<TReferenceExpression>()) {
            if (typedExpr->Reference.ColumnName == attributePath) {
                return true;
            }
        }
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

std::optional<std::string> TryCastToStringValue(const TExpressionList& exprList) noexcept
{
    return TryCastToLiteralValue(exprList).TryMoveAs<std::string>();
}

////////////////////////////////////////////////////////////////////////////////

bool IntrospectQueryForFullScan(
    const NQueryClient::NAst::TQuery* query,
    const std::string& firstKeyFieldName,
    const std::string& firstNonEvaluatedKeyFieldName)
{
    NQueryClient::TColumnSet keyColumnsSet;
    TReferenceHarvester(&keyColumnsSet).Visit(query->WherePredicate);

    bool filteredByPrimaryKey = keyColumnsSet.contains(firstNonEvaluatedKeyFieldName);
    bool orderedByPrimaryKey = false;
    if (!query->OrderExpressions.empty() &&
        !query->OrderExpressions[0].Descending)
    {
        YT_VERIFY(!query->OrderExpressions[0].Expressions.empty());
        auto* expression = query->OrderExpressions[0].Expressions[0];
        if (auto* referenceExpression = expression->As<TReferenceExpression>()) {
            orderedByPrimaryKey = referenceExpression->Reference.ColumnName == firstKeyFieldName;
        } else if (auto* aliasExpression = expression->As<TAliasExpression>();
            aliasExpression && aliasExpression->Expression->As<TReferenceExpression>())
        {
            orderedByPrimaryKey = aliasExpression->Expression->As<TReferenceExpression>()->Reference.ColumnName ==
                firstKeyFieldName;
        }
    }

    // TODO(dgolear): Enrich filter introspection with range extraction and fail if broad ranges are used.
    Y_UNUSED(orderedByPrimaryKey);
    return !filteredByPrimaryKey;
}

bool IntrospectFilterIsAlwaysFalse(NQueryClient::NAst::TExpressionPtr filterExpression)
{
    return TQueryVisitorForDefinedAlwaysFalse()
        .Run(filterExpression);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
