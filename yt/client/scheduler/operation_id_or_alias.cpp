#include "operation_id_or_alias.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

const TString OperationAliasPrefix("*");

TString GetOperationIdOrAliasContextInfo(const TOperationIdOrAlias& operationIdOrAlias)
{
    if (std::holds_alternative<TString>(operationIdOrAlias)) {
        return Format("OperationAlias: %v", operationIdOrAlias);
    } else if (std::holds_alternative<TOperationId>(operationIdOrAlias)) {
        return Format("OperationId: %v", operationIdOrAlias);
    } else {
        Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
