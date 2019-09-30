#pragma once

#include <yt/ytlib/query_client/ast.h>

namespace NYP::NServer::NQueryHelpers {

////////////////////////////////////////////////////////////////////////////////

using TReferenceMapping = std::function<NYT::NQueryClient::NAst::TExpressionPtr(
    const NYT::NQueryClient::NAst::TReference&)>;

class TQueryRewriter
{
public:
    explicit TQueryRewriter(TReferenceMapping referenceMapping);

    NYT::NQueryClient::NAst::TExpressionPtr Run(const NYT::NQueryClient::NAst::TExpressionPtr& expr);

private:
    const TReferenceMapping ReferenceMapping_;

    void Visit(NYT::NQueryClient::NAst::TExpressionPtr* expr);
    void Visit(NYT::NQueryClient::NAst::TNullableExpressionList& list);
    void Visit(NYT::NQueryClient::NAst::TExpressionList& list);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NQueryHelpers
