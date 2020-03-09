#include "query_rewriter.h"

#include <yt/ytlib/query_client/query_preparer.h>

#include <yt/core/ypath/token.h>
#include <yt/core/ypath/tokenizer.h>
#include <util/string/cast.h>

namespace NYP::NServer::NQueryHelpers {

using namespace NYT::NQueryClient::NAst;
using namespace NYT::NYPath;

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

TReferenceExpressionPtr GetFakeTableColumnReference(NYT::TObjectsHolder* holder, const TString& columnName)
{
    return holder->New<TReferenceExpression>(
        NYT::NQueryClient::NullSourceLocation,
        columnName);
}

TExpressionPtr BuildFakeTableAttributeSelector(
    NYT::TObjectsHolder* holder,
    const TYPath& attributePath,
    const THashMap<TYPath, TString>& columnNameByAttributePathFirstToken)
{
    try {
        TTokenizer tokenizer(attributePath);
        tokenizer.Advance();
        tokenizer.Expect(ETokenType::Slash);

        tokenizer.Advance();
        tokenizer.Expect(ETokenType::Literal);

        auto it = columnNameByAttributePathFirstToken.find(tokenizer.GetLiteralValue());
        if (it == columnNameByAttributePathFirstToken.end()) {
            THROW_ERROR_EXCEPTION("Attribute path starts with unsupported token %v",
                tokenizer.GetLiteralValue());
        }

        tokenizer.Advance();
        auto attributePathSuffix = ToString(tokenizer.GetInput());

        return holder->New<TFunctionExpression>(
            NYT::NQueryClient::NullSourceLocation,
            "try_get_any",
            TExpressionList{
                GetFakeTableColumnReference(holder, it->second),
                holder->New<TLiteralExpression>(
                    NYT::NQueryClient::NullSourceLocation,
                    std::move(attributePathSuffix))
            });
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing attribute path %v",
            attributePath);
    }
}

TExpressionPtr BuildFakeTableFilterExpression(
    NYT::TObjectsHolder* holder,
    const TString& filterQuery,
    const THashMap<TYPath, TString>& columnNameByAttributePathFirstToken)
{
    auto parsedQuery = ParseSource(filterQuery, NYT::NQueryClient::EParseMode::Expression);
    auto queryExpression = std::get<TExpressionPtr>(parsedQuery->AstHead.Ast);

    holder->Merge(std::move(parsedQuery->AstHead));

    auto referenceMapping = [&] (const TReference& reference) {
        if (reference.TableName) {
            THROW_ERROR_EXCEPTION("Table references are not supported");
        }
        return BuildFakeTableAttributeSelector(holder, reference.ColumnName, columnNameByAttributePathFirstToken);
    };
    TQueryRewriter rewriter(std::move(referenceMapping));

    return rewriter.Run(queryExpression);
}

} // namespace NYP::NServer::NQueryHelpers
