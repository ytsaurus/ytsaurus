#pragma once

#include "public.h"

#include <ytlib/misc/error.h>

#include <ytlib/rpc/public.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

TCallback<void (const TError& error)> CreateRpcErrorHandler(NRpc::IServiceContextPtr context);
TClosure CreateRpcSuccessHandler(NRpc::IServiceContextPtr context);

void SetRpcMutationId(NRpc::IClientRequestPtr request, const TMutationId& id);
void GenerateRpcMutationId(NRpc::IClientRequestPtr request);
TMutationId GetRpcMutationId(NRpc::IServiceContextPtr context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
