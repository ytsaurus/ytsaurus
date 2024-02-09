#include "ast_visitors.h"

namespace NYT::NQueryClient::NAst {

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

} // NYT::NQueryClient::NAst
