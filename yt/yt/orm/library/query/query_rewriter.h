#pragma once

#include "public.h"

#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/base/ast_visitors.h>

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

using TReferenceMapping = std::function<NQueryClient::NAst::TExpressionPtr(
    const NQueryClient::NAst::TReference&)>;
using TFunctionRewriter = std::function<NQueryClient::NAst::TExpressionPtr(
    NQueryClient::NAst::TFunctionExpression*)>;
using TExpressionRewriter = std::function<NQueryClient::NAst::TExpressionPtr(
    NQueryClient::NAst::TExpressionPtr)>;

NQueryClient::NAst::TExpressionPtr DummyFunctionRewriter(NQueryClient::NAst::TFunctionExpression*);
NQueryClient::NAst::TExpressionPtr DummyReferenceMapping(const NQueryClient::NAst::TReference&);
NQueryClient::NAst::TExpressionPtr DummyExpressionRewriter(NQueryClient::NAst::TExpressionPtr);


////////////////////////////////////////////////////////////////////////////////

class TQueryRewriter
    : public NQueryClient::NAst::TRewriter<TQueryRewriter>
{
public:
    explicit TQueryRewriter(
        TObjectsHolder* holder,
        TReferenceMapping referenceMapping = DummyReferenceMapping,
        TFunctionRewriter functionRewriter = DummyFunctionRewriter,
        TExpressionRewriter expressionRewriter = DummyExpressionRewriter);

    NQueryClient::NAst::TExpressionPtr Run(const NQueryClient::NAst::TExpressionPtr& expr);

    NQueryClient::NAst::TExpressionPtr OnReference(NQueryClient::NAst::TReferenceExpressionPtr referenceExpr);
    NQueryClient::NAst::TExpressionPtr OnFunction(NQueryClient::NAst::TFunctionExpressionPtr functionExpr);
    NQueryClient::NAst::TExpressionPtr OnLiteral(NQueryClient::NAst::TLiteralExpressionPtr literalExpr);
    NQueryClient::NAst::TExpressionPtr OnAlias(NQueryClient::NAst::TAliasExpressionPtr aliasExpr);
    NQueryClient::NAst::TExpressionPtr OnUnary(NQueryClient::NAst::TUnaryOpExpressionPtr unaryExpr);
    NQueryClient::NAst::TExpressionPtr OnBinary(NQueryClient::NAst::TBinaryOpExpressionPtr binaryExpr);
    NQueryClient::NAst::TExpressionPtr OnIn(NQueryClient::NAst::TInExpressionPtr inExpr);
    NQueryClient::NAst::TExpressionPtr OnBetween(NQueryClient::NAst::TBetweenExpressionPtr betweenExpr);
    NQueryClient::NAst::TExpressionPtr OnTransform(NQueryClient::NAst::TTransformExpressionPtr transformExpr);
    NQueryClient::NAst::TExpressionPtr OnCase(NQueryClient::NAst::TCaseExpressionPtr caseExpr);
    NQueryClient::NAst::TExpressionPtr OnLike(NQueryClient::NAst::TLikeExpressionPtr likeExpr);
    NQueryClient::NAst::TExpressionPtr OnQuery(NQueryClient::NAst::TQueryExpressionPtr queryExpr);

private:
    const TReferenceMapping ReferenceMapping_;
    const TFunctionRewriter FunctionRewriter_;
    const TExpressionRewriter ExpressionRewriter_;
};

////////////////////////////////////////////////////////////////////////////////

class TBitNotQueryRewriter
    : public NQueryClient::NAst::TRewriter<TBitNotQueryRewriter>
{
public:
    TBitNotQueryRewriter(
        TObjectsHolder* holder,
        std::string referenceName,
        NQueryClient::NAst::TReference targetReference,
        bool invertExpressions = true,
        std::function<void(const NQueryClient::NAst::TReference&)> onUnexpectedReference = {});

    NQueryClient::NAst::TExpressionPtr OnBinary(NQueryClient::NAst::TBinaryOpExpressionPtr binaryExpr);
    NQueryClient::NAst::TExpressionPtr OnReference(NQueryClient::NAst::TReferenceExpressionPtr referenceExpr);

private:
    NQueryClient::NAst::TReference ExpectedReference_;
    NQueryClient::NAst::TReference TargetReference_;
    bool InvertExpressions_;
    std::function<void(const NQueryClient::NAst::TReference&)> OnUnexpectedReference_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
