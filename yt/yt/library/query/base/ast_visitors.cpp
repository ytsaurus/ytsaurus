#include "ast_visitors.h"

namespace NYT::NQueryClient::NAst {

////////////////////////////////////////////////////////////////////////////////

TListContainsTrasformer::TListContainsTrasformer(
    TAstHead* head,
    const TReference& repeatedIndexedColumn,
    const TReference& unfoldedIndexerColumn)
    : TBase(head)
    , RepeatedIndexedColumn(repeatedIndexedColumn)
    , UnfoldedIndexerColumn(unfoldedIndexerColumn)
{ }

TExpressionPtr TListContainsTrasformer::OnFunction(TFunctionExpressionPtr function)
{
    if (function->FunctionName != "list_contains" ||
        function->Arguments.size() != 2)
    {
        return TBase::OnFunction(function);
    }

    auto* reference = function->Arguments[0]->As<TReferenceExpression>();
    if (reference->Reference != RepeatedIndexedColumn) {
        return TBase::OnFunction(function);
    }

    auto* newReference = Head->New<TReferenceExpression>(
        NullSourceLocation,
        UnfoldedIndexerColumn);

    return Head->New<TBinaryOpExpression>(
        NullSourceLocation,
        EBinaryOp::Equal,
        TExpressionList{newReference},
        TExpressionList{function->Arguments[1]});
}

////////////////////////////////////////////////////////////////////////////////

TTableReferenceReplacer::TTableReferenceReplacer(
    TAstHead* head,
    THashSet<TString> replacedColumns,
    const std::optional<TString>& oldAlias,
    const std::optional<TString>& newAlias)
    : TBase(head)
    , ReplacedColumns(std::move(replacedColumns))
    , OldAlias(oldAlias)
    , NewAlias(newAlias)
{ }

TExpressionPtr TTableReferenceReplacer::OnReference(TReferenceExpressionPtr reference)
{
    const auto& columnName = reference->Reference.ColumnName;
    if (OldAlias != reference->Reference.TableName ||
        !ReplacedColumns.contains(columnName))
    {
        return reference;
    }

    return Head->New<TReferenceExpression>(NullSourceLocation, columnName, NewAlias);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NAst
