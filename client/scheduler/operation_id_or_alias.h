#pragma once

#include "public.h"

#include <yt/core/misc/variant.h>

namespace NYT {
namespace NScheduler {

extern const TString OperationAliasPrefix;

////////////////////////////////////////////////////////////////////////////////

using TOperationIdOrAlias = TVariant<TOperationId, TString>;

// NB: TOperationIdOrAlias corresponds to a oneof group of fields in proto representation,
// so we use an enclosing proto message object to properly serialize or deserialize it.
template <class TProtoClass>
void FromProto(TOperationIdOrAlias* operationIdOrAlias, const TProtoClass& enclosingProtoMessage);

template <class TProtoClassPtr>
void ToProto(TProtoClassPtr enclosingProtoMessage, const TOperationIdOrAlias& operationIdOrAlias);

// Used for setting the proper context info for operations defined by id as well as operations defined by alias.
TString GetOperationIdOrAliasContextInfo(const TOperationIdOrAlias& operationIdOrAlias);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

#define OPERATION_ID_OR_ALIAS_INL_H_
#include "operation_id_or_alias-inl.h"
#undef OPERATION_ID_OR_ALIAS_INL_H_
