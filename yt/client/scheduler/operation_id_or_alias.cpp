#include "operation_id_or_alias.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

const TString OperationAliasPrefix("*");

TString GetOperationIdOrAliasContextInfo(const TOperationIdOrAlias& operationIdOrAlias)
{
    return Visit(operationIdOrAlias,
        [&] (const TString& alias) {
            return Format("OperationAlias: %v", operationIdOrAlias);
        },
        [&] (const TOperationId& operationId) {
            return Format("OperationId: %v", operationIdOrAlias);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
