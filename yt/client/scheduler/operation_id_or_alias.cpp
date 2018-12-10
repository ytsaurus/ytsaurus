#include "operation_id_or_alias.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

const TString OperationAliasPrefix("*");

TString GetOperationIdOrAliasContextInfo(const TOperationIdOrAlias& operationIdOrAlias)
{
    return Format("Operation%v: %v",
        operationIdOrAlias.Is<TOperationId>() ? "Id" : "Alias",
        operationIdOrAlias);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
