#include "operation_id_or_alias.h"

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

const TString OperationAliasPrefix("*");

TOperationIdOrAlias::TOperationIdOrAlias(TOperationId id)
    : Payload(id)
{ }

TOperationIdOrAlias::TOperationIdOrAlias(TString alias)
    : Payload(std::move(alias))
{ }

void FormatValue(TStringBuilderBase* builder, const TOperationIdOrAlias& operationIdOrAlias, TStringBuf /*format*/)
{
    Visit(operationIdOrAlias.Payload,
        [&] (const TString& alias) {
            builder->AppendFormat("%v%v", OperationAliasPrefix, alias);
        },
        [&] (const TOperationId& operationId) {
            builder->AppendFormat("%v", operationId);
        });
}

TString ToString(const TOperationIdOrAlias& operationIdOrAlias)
{
    return ToStringViaBuilder(operationIdOrAlias);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
