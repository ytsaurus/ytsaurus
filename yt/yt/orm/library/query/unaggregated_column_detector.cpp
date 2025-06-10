#include "unaggregated_column_detector.h"

#include <yt/yt/library/query/base/ast_visitors.h>
#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/base/functions.h>

namespace NYT::NOrm::NServer::NObjects {

using namespace NQueryClient::NAst;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TUnaggregatedColumnDetector
    : public TAstVisitor<TUnaggregatedColumnDetector>
{
public:
    using TBase = TAstVisitor<TUnaggregatedColumnDetector>;

    TUnaggregatedColumnDetector(const NQueryClient::TConstTypeInferrerMapPtr functions)
        : Functions_(std::move(functions))
    { }

    void OnReference(TReferenceExpressionPtr /*referenceExpr*/)
    {
        if (AggregationFunctionsDepth_ == 0) {
            HasUnaggregatedColumn_ = true;
        }
    }

    void OnFunction(TFunctionExpressionPtr functionExpr)
    {
        bool isAggregated = Functions_->GetFunction(functionExpr->FunctionName)->IsAggregate();
        if (isAggregated) {
            AggregationFunctionsDepth_++;
        }
        TBase::OnFunction(functionExpr);
        if (isAggregated) {
            AggregationFunctionsDepth_--;
        }
    }

    bool HasUnaggregatedColumn(NQueryClient::NAst::TExpressionPtr expression)
    {
        Visit(expression);
        return HasUnaggregatedColumn_;
    }

private:
    const NQueryClient::TConstTypeInferrerMapPtr Functions_;
    int AggregationFunctionsDepth_ = 0;
    bool HasUnaggregatedColumn_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

bool HasUnaggregatedColumn(NQueryClient::NAst::TExpressionPtr expression)
{
    TUnaggregatedColumnDetector detector(NQueryClient::GetBuiltinTypeInferrers());
    return detector.HasUnaggregatedColumn(expression);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
