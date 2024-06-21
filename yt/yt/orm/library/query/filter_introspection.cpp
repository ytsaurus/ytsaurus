#include "filter_introspection.h"

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
    explicit TQueryVisitorForDefinedReference(
        const TYPath& referenceName,
        const std::optional<TString>& tableName,
        bool allowValueRange)
        : ReferenceName_(referenceName)
        , TableName_(tableName)
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
                return IsTargetReference(binaryExpr->Lhs) || IsTargetReference(binaryExpr->Rhs);
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
        return IsTargetReference(inExpr->Expr);
    }

    bool OnBetween(TBetweenExpressionPtr /*betweenExpr*/)
    {
        return false;
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
    const TYPath& ReferenceName_;
    const std::optional<TString>& TableName_;
    const bool AllowValueRange_;

    using TBaseAstVisitor<bool, TQueryVisitorForDefinedReference>::Visit;

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
    : public TAstVisitor<TQueryVisitorForAttributeReferences>
{
public:
    explicit TQueryVisitorForAttributeReferences(std::function<void(TString)> inserter)
        : Inserter_(std::move(inserter))
    { }

    void Run(TExpressionPtr expression)
    {
        Visit(expression);
    }

    void OnReference(TReferenceExpressionPtr referenceExpr)
    {
        YT_VERIFY(!referenceExpr->Reference.TableName);
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
    std::function<void(TString)> Inserter_;

    using TAstVisitor<TQueryVisitorForAttributeReferences>::Visit;
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
    return TQueryVisitorForDefinedAttributeValue(attributePath).Run(queryExpression);
}

////////////////////////////////////////////////////////////////////////////////

bool IntrospectFilterForDefinedReference(
    TExpressionPtr filterExpression,
    const TYPath& referenceName,
    const std::optional<TString>& tableName,
    bool allowValueRange)
{
    return TQueryVisitorForDefinedReference(referenceName, tableName, allowValueRange)
        .Run(filterExpression);
}

////////////////////////////////////////////////////////////////////////////////

void ExtractFilterAttributeReferences(const TString& filterQuery, std::function<void(TString)> inserter)
{
    if (!filterQuery) {
        return;
    }
    auto parsedQuery = ParseSource(filterQuery, NQueryClient::EParseMode::Expression);
    auto* queryExpression = std::get<TExpressionPtr>(parsedQuery->AstHead.Ast);

    TQueryVisitorForAttributeReferences(std::move(inserter)).Run(queryExpression);
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
