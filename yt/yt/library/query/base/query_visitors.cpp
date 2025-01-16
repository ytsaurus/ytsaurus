#include "query_visitors.h"
#include "ast.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TConstExpressionPtr TSelfifyRewriter::OnReference(const TReferenceExpression* reference)
{
    auto it = ForeignReferenceToIndexMap.find(reference->ColumnName);
    if (it == ForeignReferenceToIndexMap.end()) {
        Success = false;
        return reference;
    }
    return SelfEquations[it->second].Expression;
}

TConstExpressionPtr TAddAliasRewriter::OnReference(const TReferenceExpression* reference)
{
    auto aliasedReference = NAst::InferColumnName(NAst::TReference(reference->ColumnName, Alias));
    return New<TReferenceExpression>(reference->LogicalType, aliasedReference);
}

////////////////////////////////////////////////////////////////////////////////

TReferenceHarvester::TReferenceHarvester(TColumnSet* storage)
    : Storage_(storage)
{ }

void TReferenceHarvester::OnReference(const TReferenceExpression* referenceExpr)
{
    Storage_->insert(referenceExpr->ColumnName);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
