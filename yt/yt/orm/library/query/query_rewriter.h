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

NQueryClient::NAst::TExpressionPtr DummyFunctionRewriter(NQueryClient::NAst::TFunctionExpression*);
NQueryClient::NAst::TExpressionPtr DummyReferenceMapping(const NQueryClient::NAst::TReference&);

////////////////////////////////////////////////////////////////////////////////

class TQueryRewriter
    : public NQueryClient::NAst::TRewriter<TQueryRewriter>
{
public:
    explicit TQueryRewriter(
        TObjectsHolder* holder,
        TReferenceMapping referenceMapping = DummyReferenceMapping,
        TFunctionRewriter functionRewriter = DummyFunctionRewriter);

    NQueryClient::NAst::TExpressionPtr Run(const NQueryClient::NAst::TExpressionPtr& expr);

    NQueryClient::NAst::TExpressionPtr OnReference(NQueryClient::NAst::TReferenceExpressionPtr referenceExpr);
    NQueryClient::NAst::TExpressionPtr OnFunction(NQueryClient::NAst::TFunctionExpressionPtr functionExpr);

private:
    const TReferenceMapping ReferenceMapping_;
    const TFunctionRewriter FunctionRewriter_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
