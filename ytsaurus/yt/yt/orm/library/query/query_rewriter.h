#pragma once

#include "public.h"

#include <yt/yt/library/query/base/ast.h>

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

using TReferenceMapping = std::function<NQueryClient::NAst::TExpressionPtr(
    const NQueryClient::NAst::TReference&)>;
using TFunctionRewriter = std::function<NQueryClient::NAst::TExpressionPtr(
    NQueryClient::NAst::TFunctionExpression*)>;

NQueryClient::NAst::TExpressionPtr DummyFunctionRewriter(
    NQueryClient::NAst::TFunctionExpression*);

class TQueryRewriter
{
public:
    explicit TQueryRewriter(
        TReferenceMapping referenceMapping,
        TFunctionRewriter functionRewriter = DummyFunctionRewriter);

    NQueryClient::NAst::TExpressionPtr Run(const NQueryClient::NAst::TExpressionPtr& expr);

private:
    const TReferenceMapping ReferenceMapping_;
    const TFunctionRewriter FunctionRewriter_;

    void Visit(NQueryClient::NAst::TExpressionPtr* expr);
    void Visit(NQueryClient::NAst::TNullableExpressionList& list);
    void Visit(NQueryClient::NAst::TExpressionList& list);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
