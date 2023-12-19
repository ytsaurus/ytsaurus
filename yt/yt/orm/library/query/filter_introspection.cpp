#include "filter_introspection.h"

#include <yt/yt/orm/library/attributes/attribute_path.h>

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
{
public:
    explicit TQueryVisitorForDefinedAttributeValue(const TYPath& attributePath)
        : AttributePath_(attributePath)
    { }

    TOptionalLiteralValueWrapper Visit(TExpressionPtr expr) const
    {
        auto* typedExpr = expr->As<TBinaryOpExpression>();
        if (!typedExpr) {
            return std::nullopt;
        }
        switch (typedExpr->Opcode) {
            case EBinaryOp::Equal: {
                if (IsAttributeReference(typedExpr->Lhs, AttributePath_)) {
                    if (auto value = TryCastToLiteralValue(typedExpr->Rhs)) {
                        return value;
                    }
                } else if (IsAttributeReference(typedExpr->Rhs, AttributePath_)) {
                    if (auto value = TryCastToLiteralValue(typedExpr->Lhs)) {
                        return value;
                    }
                }
                return std::nullopt;
            }
            case EBinaryOp::Or: {
                auto lhs = Visit(typedExpr->Lhs);
                auto rhs = Visit(typedExpr->Rhs);
                if (lhs == rhs) {
                    return lhs;
                } else {
                    return std::nullopt;
                }
            }
            case EBinaryOp::And: {
                auto lhs = Visit(typedExpr->Lhs);
                auto rhs = Visit(typedExpr->Rhs);
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

private:
    const TYPath& AttributePath_;

    TOptionalLiteralValueWrapper Visit(const TExpressionList& exprList) const
    {
        if (exprList.size() == 1) {
            return Visit(exprList[0]);
        }
        return std::nullopt;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TQueryVisitorForDefinedReference
{
public:
    explicit TQueryVisitorForDefinedReference(
        const TYPath& referenceName,
        const std::optional<TString>& tableName,
        bool allowValueRange)
        : ReferenceName_(referenceName)
        , TableName_(tableName)
        , AllowValueRange_(allowValueRange)
    { }

    bool Visit(TExpressionPtr expr)
    {
        if (auto* binaryExpr = expr->As<TBinaryOpExpression>()) {
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
                case EBinaryOp::Equal:
                    return IsTargetReference(binaryExpr->Lhs) || IsTargetReference(binaryExpr->Rhs);
                default:
                    return false;
            }
        } else if (auto* inExpr = expr->As<TInExpression>()) {
            return IsTargetReference(inExpr->Expr);
        }
        return false;
    }

private:
    const TYPath& ReferenceName_;
    const std::optional<TString>& TableName_;
    const bool AllowValueRange_;

    bool Visit(const TExpressionList& exprs)
    {
        if (exprs.size() != 1) {
            return false;
        }
        return Visit(exprs[0]);
    }

    bool IsTargetReference(const TExpressionList& exprs)
    {
        if (exprs.size() != 1) {
            return false;
        }
        if (auto* refExpr = exprs[0]->As<TReferenceExpression>()) {
            return refExpr->Reference.ColumnName == ReferenceName_ &&
                refExpr->Reference.TableName == TableName_;
        }
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TQueryVisitorForAttributeReferences
{
public:
    explicit TQueryVisitorForAttributeReferences(std::function<void(TString)> inserter)
        : Inserter_(std::move(inserter))
    { }

    void Visit(TExpressionPtr expr)
    {
        if (auto* typedExpr = expr->As<TLiteralExpression>()) {
        } else if (auto* typedExpr = expr->As<TReferenceExpression>()) {
            YT_VERIFY(!typedExpr->Reference.TableName);
            Inserter_(typedExpr->Reference.ColumnName);
        } else if (auto* typedExpr = expr->As<TAliasExpression>()) {
            Visit(typedExpr->Expression);
        } else if (auto* typedExpr = expr->As<TFunctionExpression>()) {
            Visit(typedExpr->Arguments);
        } else if (auto* typedExpr = expr->As<TUnaryOpExpression>()) {
            Visit(typedExpr->Operand);
        } else if (auto* typedExpr = expr->As<TBinaryOpExpression>()) {
            Visit(typedExpr->Lhs);
            Visit(typedExpr->Rhs);
        } else if (auto* typedExpr = expr->As<TInExpression>()) {
            Visit(typedExpr->Expr);
        } else if (auto* typedExpr = expr->As<TBetweenExpression>()) {
            Visit(typedExpr->Expr);
        } else if (auto* typedExpr = expr->As<TTransformExpression>()) {
            Visit(typedExpr->Expr);
            if (auto defaultExpr = typedExpr->DefaultExpr) {
                Visit(*defaultExpr);
            }
        } else {
            THROW_ERROR_EXCEPTION("Unsupported expression type");
        }
    }

private:
    std::function<void(TString)> Inserter_;

    void Visit(const TExpressionList& exprList)
    {
        for (auto* expr : exprList) {
            Visit(expr);
        }
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TOptionalLiteralValueWrapper IntrospectFilterForDefinedAttributeValue(
    const TString& filterQuery,
    const TYPath& attributePath)
{
    NAttributes::ValidateAttributePath(attributePath);
    if (!attributePath) {
        THROW_ERROR_EXCEPTION("Attribute path must be non-empty");
    }
    if (!filterQuery) {
        return std::nullopt;
    }
    auto parsedQuery = ParseSource(filterQuery, NQueryClient::EParseMode::Expression);
    auto* queryExpression = std::get<TExpressionPtr>(parsedQuery->AstHead.Ast);
    return TQueryVisitorForDefinedAttributeValue(attributePath).Visit(queryExpression);
}

////////////////////////////////////////////////////////////////////////////////

bool IntrospectFilterForDefinedReference(
    TExpressionPtr filterExpression,
    const TYPath& referenceName,
    const std::optional<TString>& tableName,
    bool allowValueRange)
{
    return TQueryVisitorForDefinedReference(referenceName, tableName, allowValueRange)
        .Visit(filterExpression);
}

////////////////////////////////////////////////////////////////////////////////

void ExtractFilterAttributeReferences(const TString& filterQuery, std::function<void(TString)> inserter)
{
    if (!filterQuery) {
        return;
    }
    auto parsedQuery = ParseSource(filterQuery, NQueryClient::EParseMode::Expression);
    auto* queryExpression = std::get<TExpressionPtr>(parsedQuery->AstHead.Ast);

    TQueryVisitorForAttributeReferences(std::move(inserter)).Visit(queryExpression);
}

////////////////////////////////////////////////////////////////////////////////

bool IsAttributeReference(const TExpressionList& exprList, const TYPath& attributePath) noexcept
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

std::optional<TString> TryCastToStringValue(const TExpressionList& exprList) noexcept
{
    return TryCastToLiteralValue(exprList).TryMoveAs<TString>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
