#pragma once

#include "public.h"

#include <ytlib/misc/error.h>

#include <ytlib/rpc/public.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

template <class TContext>
TClosure CreateRpcSuccessHandler(TIntrusivePtr<TContext> context);

template <class TContext>
TCallback<void (const TError& error)> CreateRpcErrorHandler(TIntrusivePtr<TContext> context);

void SetRpcMutationId(NRpc::IClientRequestPtr request, const TMutationId& id);
void GenerateRpcMutationId(NRpc::IClientRequestPtr request);
TMutationId GetRpcMutationId(NRpc::IServiceContextPtr context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT

#define RPC_HELPERS_INL_H_
#include "rpc_helpers-inl.h"
#undef RPC_HELPERS_INL_H_
