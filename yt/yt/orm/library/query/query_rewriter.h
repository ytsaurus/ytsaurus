#pragma once

#include "public.h"

#include <yt/yt/library/query/base/ast.h>

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

using TReferenceMapping = std::function<NYT::NQueryClient::NAst::TExpressionPtr(
    const NYT::NQueryClient::NAst::TReference&)>;
using TFunctionRewriter = std::function<NYT::NQueryClient::NAst::TExpressionPtr(
    NYT::NQueryClient::NAst::TFunctionExpression*)>;

NYT::NQueryClient::NAst::TExpressionPtr DummyFunctionRewriter(
    NYT::NQueryClient::NAst::TFunctionExpression*);

class TQueryRewriter
{
public:
    explicit TQueryRewriter(
        TReferenceMapping referenceMapping,
        TFunctionRewriter functionRewriter = DummyFunctionRewriter);

    NYT::NQueryClient::NAst::TExpressionPtr Run(const NYT::NQueryClient::NAst::TExpressionPtr& expr);

private:
    const TReferenceMapping ReferenceMapping_;
    const TFunctionRewriter FunctionRewriter_;

    void Visit(NYT::NQueryClient::NAst::TExpressionPtr* expr);
    void Visit(NYT::NQueryClient::NAst::TNullableExpressionList& list);
    void Visit(NYT::NQueryClient::NAst::TExpressionList& list);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
