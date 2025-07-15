#include "enforce_aggregate.h"

#include <yt/yt/library/query/base/ast_visitors.h>
#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/base/functions.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NQueryClient::NAst;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TUnaggregatedRewriter
    : public TRewriter<TUnaggregatedRewriter>
{
public:
    using TBase = TRewriter<TUnaggregatedRewriter>;

    TUnaggregatedRewriter(
        TObjectsHolder* objectsHolder,
        const NQueryClient::TConstTypeInferrerMapPtr functions)
        : TBase(objectsHolder)
        , Functions_(std::move(functions))
    { }

    TExpressionPtr OnReference(TReferenceExpressionPtr referenceExpr)
    {
        if (AggregationFunctionsDepth_ == 0) {
            return Head->New<TFunctionExpression>(
                NQueryClient::NullSourceLocation,
                "first",
                TExpressionList{referenceExpr});
        }
        return referenceExpr;
    }

    TExpressionPtr OnFunction(TFunctionExpressionPtr functionExpr)
    {
        bool isAggregated = Functions_->GetFunction(functionExpr->FunctionName)->IsAggregate();
        if (isAggregated) {
            AggregationFunctionsDepth_++;
        }
        auto result = TBase::OnFunction(functionExpr);
        if (isAggregated) {
            AggregationFunctionsDepth_--;
        }
        return result;
    }

private:
    const NQueryClient::TConstTypeInferrerMapPtr Functions_;
    int AggregationFunctionsDepth_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

NQueryClient::NAst::TExpressionPtr EnforceAggregate(
    TObjectsHolder* objectsHolder,
    NQueryClient::NAst::TExpressionPtr expr)
{
    return TUnaggregatedRewriter(objectsHolder, NQueryClient::GetBuiltinTypeInferrers()).Visit(expr);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
