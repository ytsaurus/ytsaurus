#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/rpc/public.h>
#include <core/rpc/rpc.pb.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

template <class TContext>
TClosure CreateRpcSuccessHandler(TIntrusivePtr<TContext> context);

template <class TContext>
TCallback<void (const TError& error)> CreateRpcErrorHandler(TIntrusivePtr<TContext> context);

NMetaState::TMutationId GenerateMutationId();

TMutationId GetMutationId(NRpc::IServiceContextPtr context);
TMutationId GetMutationId(const NRpc::NProto::TRequestHeader& header);

void GenerateMutationId(NRpc::IClientRequestPtr request);
void SetMutationId(NRpc::NProto::TRequestHeader* header, const TMutationId& id);
void SetMutationId(NRpc::IClientRequestPtr request, const TMutationId& id);
void SetOrGenerateMutationId(NRpc::IClientRequestPtr request, const TMutationId& id);

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT

#define RPC_HELPERS_INL_H_
#include "rpc_helpers-inl.h"
#undef RPC_HELPERS_INL_H_
